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
  ) where

import Control.Exception (Exception, mask_, throwIO)
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

import Tv.Types (ColType (..), TblOps (..), Column)

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
  -- duckdb_database / duckdb_connection are void* handles. duckdb_open wants
  -- a Ptr DuckDBDatabase out-slot; we malloc each slot and attach the FFI
  -- finalizer to the slot. The underlying handle is freed by the finalizer;
  -- we leak ~16 bytes per connection for the slot itself (acceptable).
  dbSlot <- mallocBytes (sizeOf (undefined :: Ptr ())) :: IO (Ptr DuckDBDatabase)
  cnSlot <- mallocBytes (sizeOf (undefined :: Ptr ())) :: IO (Ptr DuckDBConnection)
  poke (castPtr dbSlot :: Ptr (Ptr ())) nullPtr
  poke (castPtr cnSlot :: Ptr (Ptr ())) nullPtr
  rc <- withCString path $ \cpath -> C.c_duckdb_open cpath dbSlot
  when (rc /= DuckDBSuccess) $ do
    free dbSlot
    free cnSlot
    throwIO (DuckDBError ("duckdb_open failed for " ++ path))
  db <- peek dbSlot
  rc2 <- C.c_duckdb_connect db cnSlot
  when (rc2 /= DuckDBSuccess) $ do
    C.c_duckdb_close dbSlot
    free dbSlot
    free cnSlot
    throwIO (DuckDBError "duckdb_connect failed")
  -- Use concurrent (Haskell-side) finalizers so we can combine them with
  -- our free(slot) cleanup. GHC.ForeignPtr rejects mixing C and Haskell
  -- finalizers on the same ForeignPtr.
  dbFp <- newForeignPtr_ dbSlot
  cnFp <- newForeignPtr_ cnSlot
  addForeignPtrConcFinalizer dbFp $ do
    C.c_duckdb_close dbSlot
    free dbSlot
  -- cnFp's finalizer closes the connection, frees its slot, then touches
  -- dbFp. The touch keeps the db alive until after the connection is
  -- disconnected — ForeignPtr ordering is otherwise unspecified.
  addForeignPtrConcFinalizer cnFp $ do
    C.c_duckdb_disconnect cnSlot
    free cnSlot
    touchForeignPtr dbFp
  pure (Conn dbFp cnFp)

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

-- | Materialize an entire 'Result' into a 2D text grid and wrap it as a
-- 'TblOps'. Rows are read eagerly (reasonable for the default ≤1000-row
-- limit enforced by callers). Mutation fields are no-ops — this is a
-- read-only view suitable for the MVP runtime.
mkDbOps :: Result -> IO TblOps
mkDbOps res = do
  let names = resColNames res
      tys   = resColTypes res
      nc    = V.length names
  cs <- chunks res
  -- force every chunk size AND every cell: build one Vector Text per row
  let readChunk ch =
        let sz   = chunkSize ch
            cvs  = V.generate nc (chunkColumn ch)
        in V.generate sz $ \r -> V.generate nc $ \c -> readCellAny (cvs V.! c) r
      rows = V.concat (map readChunk cs)
      nr = V.length rows
  let ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = names
        , _tblTotalRows   = nr
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = \i -> if i < 0 || i >= nc then CTOther else tys V.! i
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure ((rows V.! r) V.! c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  pure ops

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
