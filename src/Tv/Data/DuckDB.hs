{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE PatternSynonyms #-}
-- | Zero-copy DuckDB FFI primitives.
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
--
-- This module is the FFI layer only. Higher-level PRQL-driven operations
-- (requery, queryCount, AdbcTable wiring) live in Tv.Data.ADBC.Table.
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
  , cellInt
  , cellDbl
  , cellText
  , cellAny
  , cellDate
  , cellTime
  , cellTs
  ) where

import Control.Exception (Exception, mask_, throwIO)
import Control.Monad (when)
import Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import Data.Int (Int16, Int32, Int64, Int8)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Time.Calendar (addDays, fromGregorian, toGregorian)
import Text.Printf (printf)
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

import Tv.Types (ColType(..))

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
cellInt :: ColumnView -> Int -> Maybe Int64
cellInt cv row
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
{-# NOINLINE cellInt #-}

-- | Read a floating-point cell (FLOAT/DOUBLE).
cellDbl :: ColumnView -> Int -> Maybe Double
cellDbl cv row
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
{-# NOINLINE cellDbl #-}

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
cellText :: ColumnView -> Int -> Maybe Text
cellText cv row
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
{-# NOINLINE cellText #-}

-- ============================================================================
-- Type mapping
-- ============================================================================

-- | Render any cell to text. NULLs become "".
cellAny :: ColumnView -> Int -> Text
cellAny cv row = case cvType cv of
  ColTypeInt       -> maybe "" (T.pack . show) (cellInt cv row)
  ColTypeBool      -> case cellInt cv row of
                        Just 0 -> "false"; Just _ -> "true"; Nothing -> ""
  -- 3-decimal precision matches Lean's format_cell_view
  -- (`format_struct_cell` passes decimals=3). Used by status-bar aggregates
  -- and any generic cellStr consumer.
  ColTypeFloat     -> maybe "" (T.pack . printf "%.3f") (cellDbl cv row)
  ColTypeStr       -> maybe "" id (cellText cv row)
  ColTypeDate      -> maybe "" id (cellDate cv row)
  ColTypeTime      -> maybe "" id (cellTime cv row)
  ColTypeTimestamp -> maybe "" id (cellTs cv row)
  _                -> maybe "" id (cellText cv row)

-- | DuckDB DATE: int32 days since 1970-01-01. Format YYYY-MM-DD.
cellDate :: ColumnView -> Int -> Maybe Text
cellDate cv row
  | row < 0 || row >= cvSize cv || cvRawType cv /= DuckDBTypeDate = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok then pure Nothing else do
        d <- peekElemOff (castPtr (cvData cv) :: Ptr Int32) row
        let day = addDays (fromIntegral d) (fromGregorian 1970 1 1)
            (y, m, dd) = toGregorian day
        pure (Just (T.pack (printf "%04d-%02d-%02d" y m dd)))
{-# NOINLINE cellDate #-}

-- | DuckDB TIME: int64 microseconds since midnight. Format HH:MM:SS.
cellTime :: ColumnView -> Int -> Maybe Text
cellTime cv row
  | row < 0 || row >= cvSize cv || cvRawType cv /= DuckDBTypeTime = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok then pure Nothing else do
        us <- peekElemOff (castPtr (cvData cv) :: Ptr Int64) row
        let s = us `div` 1000000
            (h, sm) = (s `div` 3600 `mod` 24, s `mod` 3600)
            (mi, se) = (sm `div` 60, sm `mod` 60)
        pure (Just (T.pack (printf "%02d:%02d:%02d" h mi se)))
{-# NOINLINE cellTime #-}

-- | DuckDB TIMESTAMP / TIMESTAMP_TZ: int64 microseconds since 1970-01-01.
-- Format YYYY-MM-DD HH:MM:SS.
cellTs :: ColumnView -> Int -> Maybe Text
cellTs cv row
  | row < 0 || row >= cvSize cv = Nothing
  | rt /= DuckDBTypeTimestamp && rt /= DuckDBTypeTimestampTz = Nothing
  | otherwise = U.unsafePerformIO $ do
      ok <- isValid (cvValidity cv) row
      if not ok then pure Nothing else do
        us <- peekElemOff (castPtr (cvData cv) :: Ptr Int64) row
        let totalSec = us `div` 1000000
            (days, secOfDay) = totalSec `divMod` 86400
            day = addDays (fromIntegral days) (fromGregorian 1970 1 1)
            (y, m, dd) = toGregorian day
            (h, sm) = (secOfDay `div` 3600, secOfDay `mod` 3600)
            (mi, se) = (sm `div` 60, sm `mod` 60)
        pure (Just (T.pack (printf "%04d-%02d-%02d %02d:%02d:%02d" y m dd h mi se)))
  where rt = cvRawType cv
{-# NOINLINE cellTs #-}

duckTypeToCol :: DuckDBType -> ColType
duckTypeToCol t
  | t == DuckDBTypeBoolean = ColTypeBool
  | t == DuckDBTypeTinyInt = ColTypeInt
  | t == DuckDBTypeSmallInt = ColTypeInt
  | t == DuckDBTypeInteger = ColTypeInt
  | t == DuckDBTypeBigInt = ColTypeInt
  | t == DuckDBTypeUTinyInt = ColTypeInt
  | t == DuckDBTypeUSmallInt = ColTypeInt
  | t == DuckDBTypeUInteger = ColTypeInt
  | t == DuckDBTypeUBigInt = ColTypeInt
  | t == DuckDBTypeFloat = ColTypeFloat
  | t == DuckDBTypeDouble = ColTypeFloat
  | t == DuckDBTypeDecimal = ColTypeDecimal
  | t == DuckDBTypeVarchar = ColTypeStr
  | t == DuckDBTypeDate = ColTypeDate
  | t == DuckDBTypeTime = ColTypeTime
  | t == DuckDBTypeTimestamp = ColTypeTimestamp
  | t == DuckDBTypeTimestampTz = ColTypeTimestamp
  | otherwise = ColTypeOther
