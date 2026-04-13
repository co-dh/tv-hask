{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternSynonyms #-}
-- | Zero-copy DuckDB backend.
--
-- Streams query results as a lazy list of 'DataChunk's wrapped in
-- 'ForeignPtr's. Each chunk is a direct handle to DuckDB's internal vector
-- storage (2048 rows by default); readers @peekElemOff@ straight into those
-- buffers. The only per-row allocation is the 'ForeignPtr' wrapper per chunk.
--
-- C-side lifecycle is managed by 'ForeignPtr' finalizers:
--   * 'Conn'      owns a @duckdb_database@ and @duckdb_connection@; the
--                 connection retains the database via a concurrent finalizer
--                 so the close ordering is correct.
--   * 'Result'    owns a heap-allocated 'DuckDBResult' struct freed with
--                 @duckdb_destroy_result@; retains 'Conn'.
--   * 'DataChunk' finalizer calls @duckdb_destroy_data_chunk@; retains
--                 'Result'.
module Tv.Data.DuckDB
  ( Conn
  , Result
  , DataChunk
  , ColumnView (..)
  , connect
  , disconnect
  , query
  , columnNames
  , columnTypes
  , chunks
  , chunkSize
  , chunkColumn
  , readCellInt
  , readCellDouble
  , readCellText
  , readCellAny
  , mkDbOps
  , mkDbOpsQ
  , requery
  , queryCount
  , prqlQuery
  , prqlLimit
  , stripSemi
  ) where

import Control.Exception (Exception, SomeException, mask_, throwIO, try)
import Control.Monad (when)
import Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word16, Word32, Word64, Word8)
import Foreign.C.String (peekCString, withCString)
import Foreign.ForeignPtr
import GHC.ForeignPtr (addForeignPtrConcFinalizer)
import Foreign.Marshal.Alloc (alloca, free, mallocBytes)
import Foreign.Ptr (Ptr, castPtr, nullPtr, plusPtr)
import Foreign.Storable (Storable (..))
import qualified System.IO.Unsafe as U

import qualified Tv.Data.Prql as Prql
import qualified Database.DuckDB.FFI as C
import Database.DuckDB.FFI
  ( DuckDBConnection
  , DuckDBDataChunk
  , DuckDBDatabase
  , DuckDBIdx
  , DuckDBResult (..)
  , DuckDBType
  , pattern DuckDBSuccess
  , pattern DuckDBTypeBigInt
  , pattern DuckDBTypeBoolean
  , pattern DuckDBTypeDate
  , pattern DuckDBTypeDecimal
  , pattern DuckDBTypeDouble
  , pattern DuckDBTypeFloat
  , pattern DuckDBTypeInteger
  , pattern DuckDBTypeSmallInt
  , pattern DuckDBTypeTime
  , pattern DuckDBTypeTimestamp
  , pattern DuckDBTypeTimestampTz
  , pattern DuckDBTypeTinyInt
  , pattern DuckDBTypeUBigInt
  , pattern DuckDBTypeUInteger
  , pattern DuckDBTypeUSmallInt
  , pattern DuckDBTypeUTinyInt
  , pattern DuckDBTypeVarchar
  )

import Tv.Types hiding (DataChunk)
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- ============================================================================
-- Handle types
-- ============================================================================

-- | Live database + connection. The connection ForeignPtr carries a
-- concurrent finalizer that also touches the database, ensuring we close the
-- connection strictly before the database.
data Conn = Conn
  { connDb  :: !(ForeignPtr DuckDBDatabase)
  , connHdl :: !(ForeignPtr DuckDBConnection)
  }

-- | A query result. Holds a heap-allocated 'DuckDBResult' struct whose
-- finalizer runs @duckdb_destroy_result@. 'Conn' is retained via a
-- concurrent finalizer.
data Result = Result
  { resPtr      :: !(ForeignPtr DuckDBResult)
  , resConn     :: !Conn
  , resColNames :: !(Vector Text)
  , resColTypes :: !(Vector ColType)
  , resRawTypes :: !(Vector DuckDBType)
  }

-- | One DuckDB data chunk (≤ 2048 rows). The ForeignPtr finalizer runs
-- @duckdb_destroy_data_chunk@ and retains the parent 'Result'.
data DataChunk = DataChunk
  { chunkFp    :: !(ForeignPtr ())
  , chunkOwner :: !Result
  }

-- | Zero-copy view into one column of one chunk.
--
-- @cvData@ is the raw data pointer from @duckdb_vector_get_data@; element
-- layout depends on @cvRawType@. @cvValidity@ may be NULL, meaning "all
-- rows valid".
data ColumnView = ColumnView
  { cvType     :: !ColType
  , cvRawType  :: !DuckDBType
  , cvSize     :: !Int
  , cvData     :: !(Ptr ())
  , cvValidity :: !(Ptr Word64)
  , cvChunk    :: !DataChunk  -- retains chunk for lifetime of reads
  }

data DuckDBError = DuckDBError String deriving Show
instance Exception DuckDBError

-- ============================================================================
-- Connect / disconnect
-- ============================================================================

-- | Open (":memory:" for in-memory) and connect. The returned 'Conn' owns
-- both handles; drop it (or call 'disconnect') to release.
connect :: FilePath -> IO Conn
connect path = mask_ $ do
  dbSlot <- mallocBytes (sizeOf (undefined :: Ptr ())) :: IO (Ptr DuckDBDatabase)
  cnSlot <- mallocBytes (sizeOf (undefined :: Ptr ())) :: IO (Ptr DuckDBConnection)
  poke (castPtr dbSlot :: Ptr (Ptr ())) nullPtr
  poke (castPtr cnSlot :: Ptr (Ptr ())) nullPtr
  rc <- withCString path $ \cpath -> C.c_duckdb_open cpath dbSlot
  when (rc /= DuckDBSuccess) $ do
    free dbSlot; free cnSlot
    throwIO (DuckDBError ("duckdb_open failed for " ++ path))
  db <- peek dbSlot
  rc2 <- C.c_duckdb_connect db cnSlot
  when (rc2 /= DuckDBSuccess) $ do
    C.c_duckdb_close dbSlot; free dbSlot; free cnSlot
    throwIO (DuckDBError "duckdb_connect failed")
  dbFp <- newForeignPtr_ dbSlot
  cnFp <- newForeignPtr_ cnSlot
  addForeignPtrConcFinalizer dbFp $ do
    C.c_duckdb_close dbSlot; free dbSlot
  addForeignPtrConcFinalizer cnFp $ do
    C.c_duckdb_disconnect cnSlot; free cnSlot; touchForeignPtr dbFp
  let conn = Conn dbFp cnFp
  -- Cap DuckDB's buffer pool. Default is 80% of system RAM — causes OOM
  -- when multiple in-memory databases coexist (tests, derive, loadFromUri).
  _ <- query conn "SET memory_limit='2GB'"
  pure conn

-- | Explicit disconnect. Normally the GC handles it.
disconnect :: Conn -> IO ()
disconnect c = do
  finalizeForeignPtr (connHdl c)
  finalizeForeignPtr (connDb c)

-- ============================================================================
-- Query
-- ============================================================================

-- | Execute a SQL query. Column metadata is extracted eagerly; row data
-- streams lazily via 'chunks'.
query :: Conn -> Text -> IO Result
query conn sql = mask_ $ do
  resSlot <- mallocBytes (sizeOf (undefined :: DuckDBResult)) :: IO (Ptr DuckDBResult)
  zeroBytes (castPtr resSlot) (sizeOf (undefined :: DuckDBResult))
  rc <- withForeignPtr (connHdl conn) $ \slot -> do
    cn <- peek slot
    withCString (T.unpack sql) $ \csql ->
      C.c_duckdb_query cn csql resSlot
  when (rc /= DuckDBSuccess) $ do
    err <- C.c_duckdb_result_error resSlot
    msg <-
      if err == nullPtr
        then pure "duckdb_query: unknown error"
        else peekCString err
    C.c_duckdb_destroy_result resSlot
    free resSlot
    throwIO (DuckDBError msg)
  ncol <- fromIntegral <$> C.c_duckdb_column_count resSlot :: IO Int
  let ixs = V.enumFromN 0 ncol
  names <-
    V.mapM
      ( \i -> do
          cs <- C.c_duckdb_column_name resSlot (fromIntegral i :: DuckDBIdx)
          if cs == nullPtr
            then pure T.empty
            else T.pack <$> peekCString cs
      )
      ixs
  rawTys <- V.mapM (\i -> C.c_duckdb_column_type resSlot (fromIntegral i :: DuckDBIdx)) ixs
  let tys = V.map duckTypeToCol rawTys
  resFp <- newForeignPtr_ resSlot
  addForeignPtrConcFinalizer resFp $ do
    C.c_duckdb_destroy_result resSlot
    free resSlot
    touchForeignPtr (connHdl conn)
  pure (Result resFp conn names tys rawTys)

zeroBytes :: Ptr Word8 -> Int -> IO ()
zeroBytes p n = go 0
  where
    go i
      | i >= n = pure ()
      | otherwise = pokeByteOff p i (0 :: Word8) >> go (i + 1)

columnNames :: Result -> Vector Text
columnNames = resColNames

columnTypes :: Result -> Vector ColType
columnTypes = resColTypes

-- | Lazily fetch chunks. 'unsafeInterleaveIO' lets callers consume only
-- the first few without forcing the rest.
chunks :: Result -> IO [DataChunk]
chunks res = go
  where
    go = U.unsafeInterleaveIO $ do
      raw <- withForeignPtr (resPtr res) C.c_duckdb_fetch_chunk
      if raw == nullPtr
        then pure []
        else do
          fp <- newForeignPtr_ (castPtr raw :: Ptr ())
          -- duckdb_destroy_data_chunk takes Ptr DuckDBDataChunk (pointer to
          -- the chunk pointer). Use a tiny alloca'd slot to pass it.
          addForeignPtrConcFinalizer fp $ do
            alloca $ \slot -> do
              poke slot raw
              C.c_duckdb_destroy_data_chunk slot
            touchForeignPtr (resPtr res)
          let c = DataChunk fp res
          rest <- go
          pure (c : rest)

-- | Number of tuples in this chunk.
chunkSize :: DataChunk -> Int
chunkSize c =
  U.unsafePerformIO $
    withForeignPtr (chunkFp c) $ \p ->
      fromIntegral <$> C.c_duckdb_data_chunk_get_size (castPtr p)
{-# NOINLINE chunkSize #-}

-- | Get a zero-copy view over one column of this chunk.
chunkColumn :: DataChunk -> Int -> ColumnView
chunkColumn chunk colIdx = U.unsafePerformIO $
  withForeignPtr (chunkFp chunk) $ \p -> do
    let cp = castPtr p :: DuckDBDataChunk
    sz <- fromIntegral <$> C.c_duckdb_data_chunk_get_size cp
    vec <- C.c_duckdb_data_chunk_get_vector cp (fromIntegral colIdx)
    dataP <- C.c_duckdb_vector_get_data vec
    valP <- C.c_duckdb_vector_get_validity vec
    let rawT = resRawTypes (chunkOwner chunk) V.! colIdx
        cty  = resColTypes (chunkOwner chunk) V.! colIdx
    pure (ColumnView cty rawT sz dataP valP chunk)
{-# NOINLINE chunkColumn #-}

-- ============================================================================
-- Cell readers (zero-copy)
-- ============================================================================

isValid :: Ptr Word64 -> Int -> IO Bool
isValid vp row
  | vp == nullPtr = pure True
  | otherwise = do
      let (q, r) = row `divMod` 64
      w <- peekElemOff vp q
      pure ((w `shiftR` r) .&. 1 == 1)

-- | Read an integer cell. Handles all DuckDB integer widths + bool.
readCellInt :: ColumnView -> Int -> Maybe Int64
readCellInt cv row
  | row < 0 || row >= cvSize cv = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok
        then pure Nothing
        else do
          let d = cvData cv
              t = cvRawType cv
          case () of
            _
              | t == DuckDBTypeBigInt ->
                  Just <$> peekElemOff (castPtr d :: Ptr Int64) row
              | t == DuckDBTypeInteger ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Int32) row
              | t == DuckDBTypeSmallInt ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Int16) row
              | t == DuckDBTypeTinyInt ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Int8) row
              | t == DuckDBTypeUBigInt ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Word64) row
              | t == DuckDBTypeUInteger ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Word32) row
              | t == DuckDBTypeUSmallInt ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Word16) row
              | t == DuckDBTypeUTinyInt ->
                  Just . fromIntegral <$> peekElemOff (castPtr d :: Ptr Word8) row
              | t == DuckDBTypeBoolean -> do
                  b <- peekElemOff (castPtr d :: Ptr Word8) row
                  pure (Just (if b /= 0 then 1 else 0))
              | otherwise -> pure Nothing
{-# NOINLINE readCellInt #-}

-- | Read a floating-point cell (FLOAT/DOUBLE).
readCellDouble :: ColumnView -> Int -> Maybe Double
readCellDouble cv row
  | row < 0 || row >= cvSize cv = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok
        then pure Nothing
        else
          let d = cvData cv
              t = cvRawType cv
           in if t == DuckDBTypeDouble
                then Just <$> peekElemOff (castPtr d :: Ptr Double) row
                else
                  if t == DuckDBTypeFloat
                    then do
                      f <- peekElemOff (castPtr d :: Ptr Float) row
                      pure (Just (realToFrac f))
                    else pure Nothing
{-# NOINLINE readCellDouble #-}

-- | Read a VARCHAR cell. Each element is a 16-byte @duckdb_string_t@:
--
-- > struct {
-- >   uint32_t length;
-- >   union {
-- >     struct { char prefix[4]; char *ptr; } pointer;
-- >     char inlined[12];
-- >   };
-- > };
--
-- If @length <= 12@ the data is inlined at offset 4. Otherwise bytes 8..15
-- hold a @char *@ into heap-managed string storage owned by the chunk.
readCellText :: ColumnView -> Int -> Maybe Text
readCellText cv row
  | row < 0 || row >= cvSize cv = Nothing
  | cvRawType cv /= DuckDBTypeVarchar = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok
        then pure Nothing
        else do
          let base = cvData cv `plusPtr` (row * 16)
          len32 <- peek (castPtr base :: Ptr Word32)
          let len = fromIntegral len32 :: Int
          bs <-
            if len <= 12
              then BS.packCStringLen (castPtr (base `plusPtr` 4), len)
              else do
                p <- peek (castPtr (base `plusPtr` 8) :: Ptr (Ptr Word8))
                BS.packCStringLen (castPtr p, len)
          pure (Just (TE.decodeUtf8 bs))
{-# NOINLINE readCellText #-}

-- ============================================================================
-- Type mapping
-- ============================================================================

-- | Render any cell to text. Used by 'mkDbOps' to materialize a display
-- cache without per-type branching in the caller. NULLs become "".
readCellAny :: ColumnView -> Int -> Text
readCellAny cv row = case cvType cv of
  CTInt  -> maybe "" (T.pack . show) (readCellInt cv row)
  CTBool -> case readCellInt cv row of
              Just 0 -> "false"; Just _ -> "true"; Nothing -> ""
  CTFloat -> maybe "" (T.pack . show) (readCellDouble cv row)
  CTStr  -> maybe "" id (readCellText cv row)
  _      -> maybe "" id (readCellText cv row)  -- fallback; TODO date/time fmt

-- | Zero-copy TblOps backed by DuckDB chunks. Rows are NOT materialized;
-- tblCellStr into the chunk vector storage on demand via readCellAny.
-- The chunk ForeignPtrs keep the underlying DuckDB memory alive as long as
-- the TblOps is reachable.
mkDbOps :: Result -> IO TblOps
mkDbOps res = do
  -- Read chunks once here — duckdb_fetch_chunk is streaming (single-pass),
  -- so we must avoid calling `chunks res` again in mkDbOpsQ.
  cs <- chunks res
  let nr = sum (map chunkSize cs)
  conn <- connect ":memory:"
  mkDbOpsQChunks cs res conn (Prql.Query "from df" V.empty) nr

-- | Build TblOps from a Result + connection + PRQL Query (for requery chain).
-- Matches Lean AdbcTable.ofQueryResult. The Conn is captured so sort/filter
-- can requery on the same connection where the data lives.
mkDbOpsQ :: Result -> Conn -> Prql.Query -> Int -> IO TblOps
mkDbOpsQ res conn q totalRows = do
  cs <- chunks res
  mkDbOpsQChunks cs res conn q totalRows

-- | Internal: build TblOps from pre-read chunks. Avoids double-consuming
-- the DuckDB streaming chunk iterator.
mkDbOpsQChunks :: [DataChunk] -> Result -> Conn -> Prql.Query -> Int -> IO TblOps
mkDbOpsQChunks cs res conn q totalRows = do
  let names = resColNames res
      tys   = resColTypes res
      nc    = V.length names
  let chunkVec = V.fromList cs
      sizes = V.map chunkSize chunkVec
      offsets = V.prescanl' (+) 0 sizes
      nr = V.sum sizes
      findChunk r = go 0 (V.length chunkVec - 1)
        where
          go lo hi
            | lo >= hi  = (chunkVec V.! lo, r - offsets V.! lo)
            | otherwise =
                let mid = (lo + hi + 1) `div` 2
                in if offsets V.! mid <= r then go mid hi else go lo (mid - 1)
  let ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = names
        , _tblTotalRows   = totalRows
        , _tblQueryOps    = Prql.ops q
        -- | Filter: append filter op, requery with new count. Lean Table.lean:341-344
        , _tblFilter      = \expr -> do
            let q' = Prql.pfilter q expr
            total <- queryCount conn q'
            requery conn q' total
        -- | Distinct: PRQL uniq. Lean Table.lean:346-354
        , _tblDistinct    = \col -> do
            let colName = if col >= 0 && col < nc then names V.! col else ""
            r <- prqlQuery conn (Prql.render q <> " | uniq " <> Prql.quote colName)
            case r of
              Nothing -> pure V.empty
              Just res' -> do
                cs' <- chunks res'
                if null cs' then pure V.empty
                else pure $ V.concat $ map (\ch ->
                  let sz = chunkSize ch; cv = chunkColumn ch 0
                  in V.generate sz (\i -> readCellAny cv i)) cs'
        -- | FindRow: derive row_number, filter, extract. Lean Table.lean:358-370
        , _tblFindRow     = \col val start fwd -> do
            let colName = Prql.quote (if col >= 0 && col < nc then names V.! col else "")
                prql = Prql.render q <> " | derive {_rn = row_number this} | filter ("
                       <> colName <> " == '" <> escSql val <> "') | select {_rn}"
            r <- prqlQuery conn prql
            case r of
              Nothing -> pure Nothing
              Just res' -> do
                rows <- readIntColumn res'
                let rows0 = map (\x -> x - 1) rows  -- 1-based → 0-based
                pure $ if null rows0 then Nothing
                  else if fwd
                    then case filter (>= start) rows0 of
                           (x:_) -> Just x
                           []    -> Just (head rows0)  -- wrap
                    else case filter (< start) (reverse rows0) of
                           (x:_) -> Just x
                           []    -> Just (last rows0)  -- wrap
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \i -> if i < 0 || i >= nc then CTOther else tys V.! i
        , _tblBuildFilter = buildFilterPrql
        , _tblFilterPrompt = \col typ ->
            let num = typ == "int" || typ == "float" || typ == "decimal"
                eg = if num then "e.g. " <> col <> " > 5,  " <> col <> " >= 10 && " <> col <> " < 100"
                     else "e.g. " <> col <> " == 'USD',  " <> col <> " ~= 'pattern'"
            in "PRQL filter on " <> col <> " (" <> typ <> "):  " <> eg
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc || nr == 0
              then pure T.empty
              else let (ch, local) = findChunk r
                   in pure (readCellAny (chunkColumn ch c) local)
        , _tblFetchMore   = pure Nothing
        -- | HideCols: PRQL select (keep non-hidden). Lean Table.lean:221-223
        , _tblHideCols    = \hideIdxs -> do
            let keep = V.ifilter (\i _ -> not (V.elem i hideIdxs)) names
                q' = Prql.pipe q (OpSel keep)
            r <- requery conn q' totalRows
            pure (maybe ops id r)
        -- | SortBy: PRQL sort op. Lean Table.lean:213-218
        , _tblSortBy      = \idxs asc -> do
            let sortCols = V.map (\i -> (if i >= 0 && i < nc then names V.! i else "", asc)) idxs
                q' = Prql.pipe q (OpSort sortCols)
            r <- requery conn q' totalRows
            pure (maybe ops id r)
        }
  pure ops

-- | Default row limit for PRQL queries (matches Lean prqlLimit = 1000)
prqlLimit :: Int
prqlLimit = 1000

-- | Trim trailing semicolon from compiled SQL
stripSemi :: Text -> Text
stripSemi s = let t = T.strip s in if T.isSuffixOf ";" t then T.init t else t

-- | Compile PRQL and execute on a connection. Matches Lean Prql.query.
prqlQuery :: Conn -> Text -> IO (Maybe Result)
prqlQuery conn prql = do
  mSql <- Prql.compile prql
  case mSql of
    Nothing -> pure Nothing
    Just sql -> do
      r <- try (query conn (stripSemi sql))
      case r of
        Right res -> pure (Just res)
        Left (_ :: SomeException) -> pure Nothing

-- | Execute PRQL query on conn, return new TblOps. Lean AdbcTable.requery.
requery :: Conn -> Prql.Query -> Int -> IO (Maybe TblOps)
requery conn q total = do
  let prql = Prql.render q <> " | take " <> T.pack (show prqlLimit)
  r <- prqlQuery conn prql
  case r of
    Nothing -> pure Nothing
    Just res -> Just <$> mkDbOpsQ res conn q total

-- | Query total row count on conn. Lean AdbcTable.queryCount.
queryCount :: Conn -> Prql.Query -> IO Int
queryCount conn q = do
  let prql = Prql.render q <> " | cnt"
  r <- prqlQuery conn prql
  case r of
    Nothing -> pure 0
    Just res -> do
      rows <- readIntColumn res
      pure (if null rows then 0 else head rows)

-- | Read all values from a single-column integer result as [Int].
readIntColumn :: Result -> IO [Int]
readIntColumn res = do
  cs <- chunks res
  pure $ concatMap (\ch ->
    let n = chunkSize ch; cv = chunkColumn ch 0
    in map (\i -> case readCellInt cv i of
              Just v -> fromIntegral v; Nothing -> 0) [0 .. n - 1]) cs

-- | Get row count from Result (sum of chunk sizes).
resNRows :: Result -> Int
resNRows res = U.unsafePerformIO $ do
  cs <- chunks res
  pure (sum (map chunkSize cs))

-- | Build PRQL filter expression from fzf result (Lean Types.lean:buildFilterPrql).
-- With --print-query: line 0 = typed query, lines 1+ = selected hints.
buildFilterPrql :: Text -> V.Vector Text -> Text -> Bool -> Text
buildFilterPrql col vals result isNum =
  let lns = filter (not . T.null) (T.splitOn "\n" result)
      input = case lns of (x:_) -> x; _ -> ""
      fromHints = filter (`V.elem` vals) (drop 1 lns)
      selected = if V.elem input vals && notElem input fromHints
                 then input : fromHints else fromHints
      q v = if isNum then v else "'" <> T.replace "'" "''" v <> "'"
  in case selected of
    [v] -> col <> " == " <> q v
    (_:_) -> "(" <> T.intercalate " || " (map (\v' -> col <> " == " <> q v') selected) <> ")"
    [] | not (T.null input) -> input  -- free-form PRQL expression
       | otherwise -> ""

duckTypeToCol :: DuckDBType -> ColType
duckTypeToCol t
  | t == DuckDBTypeBoolean = CTBool
  | t == DuckDBTypeTinyInt = CTInt
  | t == DuckDBTypeSmallInt = CTInt
  | t == DuckDBTypeInteger = CTInt
  | t == DuckDBTypeBigInt = CTInt
  | t == DuckDBTypeUTinyInt = CTInt
  | t == DuckDBTypeUSmallInt = CTInt
  | t == DuckDBTypeUInteger = CTInt
  | t == DuckDBTypeUBigInt = CTInt
  | t == DuckDBTypeFloat = CTFloat
  | t == DuckDBTypeDouble = CTFloat
  | t == DuckDBTypeDecimal = CTDecimal
  | t == DuckDBTypeVarchar = CTStr
  | t == DuckDBTypeDate = CTDate
  | t == DuckDBTypeTime = CTTime
  | t == DuckDBTypeTimestamp = CTTimestamp
  | t == DuckDBTypeTimestampTz = CTTimestamp
  | otherwise = CTOther
