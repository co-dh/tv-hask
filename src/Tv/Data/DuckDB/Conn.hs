{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  DuckDB connection + query execution.

  Manages a singleton in-process DuckDB connection. Materializes query
  results into random-access QueryResult (eagerly fetched chunks with
  binary search for row indexing).
-}
module Tv.Data.DuckDB.Conn where

import Prelude hiding (init)
import Tv.Prelude
import Control.Exception (SomeException, try)
import Numeric (showFFloat)
import qualified Data.Text as T
import qualified Data.Vector as V
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
  maybe (error "DuckDB: connection not initialized (call Conn.init first)") pure m

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

ncols :: QueryResult -> Int
ncols qr = V.length $ qr ^. #colNames

nrows :: QueryResult -> Int
nrows qr = qr ^. #nRows

colName :: QueryResult -> Int -> Text
colName qr i = (qr ^. #colNames) V.! i

-- | Column format char. Lean returns the Arrow ArrowSchema.format string. DuckDB
-- doesn't surface that directly, so we synthesize the first character per
-- Arrow conventions: int = 'l', float = 'g', str = 'u', bool = 'b', and
-- 'd'/'t'/'s' for decimal/time/timestamp. The downstream consumer in
-- cbits/tv_render.c reads only the first char, and all date/time types
-- (Arrow tdD / ttu / tsu:*) must start with 't' so `type_char_fmt` maps
-- them to the '@' header indicator.
colFmt :: QueryResult -> Int -> Text
colFmt qr i = T.singleton $ case (qr ^. #colTypes) V.! i of
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
colType :: QueryResult -> Int -> Text
colType qr i = ctToString $ (qr ^. #colTypes) V.! i
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

cellStr :: QueryResult -> Int -> Int -> Text
cellStr qr r c = maybe "" (\(ch, local) -> DB.cellAny (DB.chunkColumn ch c) local) (findChunk qr r)

cellInt :: QueryResult -> Int -> Int -> Int64
cellInt qr r c = maybe 0 (\(ch, local) -> fromMaybe 0 $ DB.cellInt (DB.chunkColumn ch c) local) (findChunk qr r)

cellFloat :: QueryResult -> Int -> Int -> Double
cellFloat qr r c = maybe 0 (\(ch, local) -> fromMaybe 0 $ DB.cellDbl (DB.chunkColumn ch c) local) (findChunk qr r)

-- | Locate (chunk, local row index) for a global row index. Binary search
-- over the prescanned chunk offsets.
findChunk :: QueryResult -> Int -> Maybe (DB.DataChunk, Int)
findChunk qr row
  | row < 0 || row >= qr ^. #nRows = Nothing
  | V.null (qr ^. #chunks) = Nothing
  | otherwise =
      let offs = qr ^. #offsets
          chs  = qr ^. #chunks
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
  Tc.ColTypeInt     -> maybe "" fmtIntComma (DB.cellInt cv local)
  Tc.ColTypeBool    -> maybe "" boolStr (DB.cellInt cv local)
  Tc.ColTypeFloat   -> maybe "" fmtFloat (DB.cellDbl cv local)
  Tc.ColTypeDecimal -> maybe "" fmtFloat (DB.cellDbl cv local)
  _                 -> DB.cellAny cv local
  where
    fmtFloat f = if isNaN f then "" else T.pack (showFFloat (Just prec_) f "")
    boolStr 0  = "false"
    boolStr _  = "true"

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
  let nc = V.length (qr ^. #colNames)
      types = qr ^. #colTypes
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
  let nc = V.length (qr ^. #colNames)
      types = qr ^. #colTypes
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
                      Tc.ColTypeInt -> maybe nan fromIntegral (DB.cellInt cv (local + i))
                      _             -> fromMaybe nan (DB.cellDbl cv (local + i))

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
