{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  DuckDB connection + query execution.

  Manages a singleton in-process DuckDB connection. Materializes query
  results into random-access QueryResult (eagerly fetched chunks with
  binary search for row indexing).
-}
module Tv.Data.DuckDB.Conn
  ( QueryResult(..)
  , init
  , shutdown
  , query
  , queryParam
  , ncols
  , nrows
  , colName
  , colFmt
  , cellStr
  , cellInt
  , cellFloat
  , colType
  , getConn
  , fetchRows
  , fetchHeatDoubles
  , fmtIntComma
  ) where

import Prelude hiding (init)
import Control.Exception (SomeException, try)
import Numeric (showFFloat)
import Data.IORef
import Data.Maybe (fromMaybe)
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word (Word64)
import System.IO.Unsafe (unsafePerformIO)

import qualified Tv.Data.DuckDB as DB
import qualified Tv.Types as Tc
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Global Conn — singleton in-process DuckDB connection.
{-# NOINLINE connRef #-}
connRef :: IORef (Maybe DB.Conn)
connRef = unsafePerformIO (newIORef Nothing)

getConn :: IO DB.Conn
getConn = do
  m <- readIORef connRef
  case m of
    Just c  -> pure c
    Nothing -> error "DuckDB: connection not initialized (call Conn.init first)"

-- | Query result with eagerly fetched chunks plus cached metadata. Lean's
-- opaque QueryResult wraps a C-side Arrow array cursor; we materialize the
-- DuckDB chunk list once at query time so `cellStr` can index by (row,col)
-- without re-iterating the one-shot `duckdb_fetch_chunk` cursor.
data QueryResult = QueryResult
  { chunks   :: V.Vector DB.DataChunk
  , offsets  :: V.Vector Int          -- prescan of chunk sizes: row → chunk idx
  , nRows    :: Int
  , colNames :: V.Vector Text
  , colTypes :: V.Vector Tc.ColType
  }
makeFieldLabelsNoPrefix ''QueryResult

-- | Opens an in-memory DuckDB if none is open yet. Returns ""
-- on success, an error message on failure (matching Lean's convention).
init :: IO Text
init = do
  r <- try $ do
    m <- readIORef connRef
    case m of
      Just _  -> pure ()
      Nothing -> do
        c <- DB.connect ":memory:"
        writeIORef connRef (Just c)
  case r of
    Right ()                   -> pure ""
    Left (e :: SomeException)  -> pure $ T.pack $ show e

-- | Disconnect and clear the singleton.
shutdown :: IO ()
shutdown = do
  m <- readIORef connRef
  case m of
    Just c  -> do DB.disconnect c; writeIORef connRef Nothing
    Nothing -> pure ()

-- | Run a raw SQL query (already compiled from PRQL).
query :: Text -> IO QueryResult
query sql = do
  c  <- getConn
  r  <- DB.query c sql
  cs <- DB.chunks r
  let chVec = V.fromList cs
      sizes = V.map DB.chunkSize chVec
      offs  = V.prescanl' (+) 0 sizes
      nr    = V.sum sizes
  pure QueryResult
    { chunks   = chVec
    , offsets  = offs
    , nRows    = nr
    , colNames = DB.columnNames r
    , colTypes = DB.columnTypes r
    }

-- | Parameterized query — Lean uses this for INSERT ... VALUES. For now it
-- ignores the param and runs sql directly. Rewire when a caller needs it.
queryParam :: Text -> Text -> IO QueryResult
queryParam sql _param = query sql

ncols :: QueryResult -> IO Word64
ncols qr = pure $ fromIntegral $ V.length $ colNames qr

nrows :: QueryResult -> IO Word64
nrows qr = pure $ fromIntegral $ nRows qr

colName :: QueryResult -> Word64 -> IO Text
colName qr i = pure $ colNames qr V.! fromIntegral i

-- | Column format char. Lean returns the Arrow ArrowSchema.format string. DuckDB
-- doesn't surface that directly, so we synthesize the first character per
-- Arrow conventions: int = 'l', float = 'g', str = 'u', bool = 'b', and
-- 'd'/'t'/'s' for decimal/time/timestamp. The downstream consumer in
-- cbits/tv_render.c reads only the first char, and all date/time types
-- (Arrow tdD / ttu / tsu:*) must start with 't' so `type_char_fmt` maps
-- them to the '@' header indicator.
colFmt :: QueryResult -> Word64 -> IO Text
colFmt qr i =
  let ty = colTypes qr V.! fromIntegral i
  in pure $ T.singleton $ case ty of
       Tc.ColTypeInt       -> 'l'
       Tc.ColTypeFloat     -> 'g'
       Tc.ColTypeDecimal   -> 'd'
       Tc.ColTypeStr       -> 'u'
       Tc.ColTypeDate      -> 't'
       Tc.ColTypeTime      -> 't'
       Tc.ColTypeTimestamp -> 't'
       Tc.ColTypeBool      -> 'b'
       Tc.ColTypeOther     -> '?'

-- | Column type string. Lean
-- returns snake-case names like "int"/"float"/"utf8"; we use our ColType
-- StrEnum toString to keep a single source of truth. NOTE: Lean's Arrow
-- path returns "utf8" for strings; we return "str". AdbcTable.ofResult
-- consumes this via `ColType.ofString`, which would have to know about
-- "utf8". When we port that, we'll either normalize there or patch this to
-- emit Arrow names — leaving this as-is for now so the ColType round-trip
-- stays symmetric with Tv.Types.
colType :: QueryResult -> Word64 -> IO Text
colType qr i = pure $ ctToString $ colTypes qr V.! fromIntegral i
  where
    ctToString :: Tc.ColType -> Text
    ctToString Tc.ColTypeInt       = "int"
    ctToString Tc.ColTypeFloat     = "float"
    ctToString Tc.ColTypeDecimal   = "decimal"
    ctToString Tc.ColTypeStr       = "str"
    ctToString Tc.ColTypeDate      = "date"
    ctToString Tc.ColTypeTime      = "time"
    ctToString Tc.ColTypeTimestamp = "timestamp"
    ctToString Tc.ColTypeBool      = "bool"
    ctToString Tc.ColTypeOther     = "other"

cellStr :: QueryResult -> Word64 -> Word64 -> IO Text
cellStr qr r c = do
  let r' = fromIntegral r; c' = fromIntegral c
  case findChunk qr r' of
    Nothing -> pure ""
    Just (ch, local) ->
      let cv = DB.chunkColumn ch c'
      in pure $ DB.cellAny cv local

cellInt :: QueryResult -> Word64 -> Word64 -> IO Int64
cellInt qr r c = do
  let r' = fromIntegral r; c' = fromIntegral c
  case findChunk qr r' of
    Nothing -> pure 0
    Just (ch, local) ->
      let cv = DB.chunkColumn ch c'
      in pure $ fromMaybe 0 $ DB.cellInt cv local

cellFloat :: QueryResult -> Word64 -> Word64 -> IO Double
cellFloat qr r c = do
  let r' = fromIntegral r; c' = fromIntegral c
  case findChunk qr r' of
    Nothing -> pure 0
    Just (ch, local) ->
      let cv = DB.chunkColumn ch c'
      in pure $ fromMaybe 0 $ DB.cellDbl cv local

-- | Locate (chunk, local row index) for a global row index. Binary search
-- over the prescanned chunk offsets.
findChunk :: QueryResult -> Int -> Maybe (DB.DataChunk, Int)
findChunk qr row
  | row < 0 || row >= nRows qr = Nothing
  | V.null (chunks qr) = Nothing
  | otherwise =
      let offs = offsets qr
          chs  = chunks qr
          go lo hi
            | lo >= hi  = lo
            | otherwise =
                let mid = (lo + hi + 1) `div` 2
                in if offs V.! mid <= row then go mid hi else go lo (mid - 1)
          ix    = go 0 (V.length chs - 1)
          local = row - offs V.! ix
      in Just (chs V.! ix, local)

-- | Format an integer with comma separators (replicates C fmt_int_comma).
-- 1234567 -> "1,234,567", -42 -> "-42", 0 -> "0"
fmtIntComma :: Int64 -> T.Text
fmtIntComma v
  | v < 0     = T.cons '-' (fmtIntComma (negate v))
  | otherwise = T.reverse (addCommas (T.reverse (T.pack (show v))))
  where
    addCommas t
      | T.length t <= 3 = t
      | otherwise        = let (h, rest) = T.splitAt 3 t
                           in h <> "," <> addCommas rest

-- | Format a single cell for display, given a pre-resolved 'ColumnView'
-- and the local row offset. Dispatches on column type.
formatCellFromCV :: DB.ColumnView -> Int -> Int -> Tc.ColType -> T.Text
formatCellFromCV cv local prec_ typ = case typ of
  Tc.ColTypeInt -> case DB.cellInt cv local of
    Nothing -> ""
    Just n  -> fmtIntComma n
  Tc.ColTypeFloat -> case DB.cellDbl cv local of
    Nothing -> ""
    Just f  -> if isNaN f then "" else T.pack (showFFloat (Just prec_) f "")
  Tc.ColTypeDecimal -> case DB.cellDbl cv local of
    Nothing -> ""
    Just f  -> if isNaN f then "" else T.pack (showFFloat (Just prec_) f "")
  Tc.ColTypeBool -> case DB.cellInt cv local of
    Just 0  -> "false"
    Just _  -> "true"
    Nothing -> ""
  _ -> DB.cellAny cv local

-- | Materialize a rectangular cell region as display-formatted text.
-- Column-major: outer Vector indexed by column, inner by local row.
--
-- Perf contract: resolves the starting chunk once and reuses
-- @chunkColumn@ across every cell that falls inside it. Safe-FFI cost
-- was dominant (4 safe ccalls per cellRead × N cells); this drops it
-- to 4 safe ccalls per (chunk, column). Typical visible windows
-- (≤ 1000 rows) live in one chunk, so the cross-chunk loop runs once.
fetchRows :: QueryResult -> Int -> Int -> Int -> IO (V.Vector (V.Vector T.Text))
fetchRows qr r0 r1 prec_ = do
  let nc = V.length (colNames qr)
      types = colTypes qr
  if r1 <= r0
    then pure (V.replicate nc V.empty)
    else V.generateM nc $ \c -> do
      let typ = if c < V.length types then types V.! c else Tc.ColTypeOther
      -- Resolve chunks for this column once; then batch-format inside each.
      withChunkRuns qr r0 r1 $ \ch local n ->
        let cv = DB.chunkColumn ch (fromIntegral c)
        in pure $ V.generate n $ \i -> formatCellFromCV cv (local + i) prec_ typ

-- | Fetch raw doubles for heat mode. NaN for non-numeric or null cells.
-- Non-numeric columns short-circuit to an empty Vector (the renderer's
-- 'scanHeat' only reads numeric columns and uses text min/max for
-- date-like formats), avoiding a per-cell sweep that would always
-- yield NaN.
fetchHeatDoubles :: QueryResult -> Int -> Int -> IO (V.Vector (V.Vector Double))
fetchHeatDoubles qr r0 r1 = do
  let nc = V.length (colNames qr)
      types = colTypes qr
      isNumTy t = t == Tc.ColTypeInt || t == Tc.ColTypeFloat || t == Tc.ColTypeDecimal
      nan = 0/0 :: Double
  if r1 <= r0
    then pure (V.replicate nc V.empty)
    else V.generateM nc $ \c -> do
      let typ = if c < V.length types then types V.! c else Tc.ColTypeOther
      if not (isNumTy typ)
        then pure V.empty   -- skip non-numeric; saves 4 safe FFI calls per chunk
        else withChunkRuns qr r0 r1 $ \ch local n ->
               let cv = DB.chunkColumn ch (fromIntegral c)
               in pure $ V.generate n $ \i ->
                    case typ of
                      Tc.ColTypeInt -> case DB.cellInt cv (local + i) of
                        Nothing -> nan
                        Just v  -> fromIntegral v
                      _ -> case DB.cellDbl cv (local + i) of
                        Nothing -> nan
                        Just v  -> v

-- | Walk the @[r0, r1)@ row range grouped by DuckDB chunk. Calls @f@
-- once per contiguous run of rows that live in the same chunk;
-- concatenates the produced vectors. 'chunkColumn' is the expensive
-- thing — we hand the chunk to the caller so it can hoist it.
withChunkRuns
  :: QueryResult -> Int -> Int
  -> (DB.DataChunk -> Int -> Int -> IO (V.Vector a))
  -> IO (V.Vector a)
withChunkRuns qr r0 r1 f = go V.empty r0
  where
    go acc row
      | row >= r1 = pure acc
      | otherwise = case findChunk qr row of
          Nothing -> pure acc
          Just (ch, local) ->
            let chunkEndGlobal = row - local + DB.chunkSize ch
                stop           = min r1 chunkEndGlobal
                n              = stop - row
            in do chunk <- f ch local n
                  go (acc V.++ chunk) stop
