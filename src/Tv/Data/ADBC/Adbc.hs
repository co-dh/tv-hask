{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  Lean-shaped ADBC shim over DuckDB.

  This module mirrors the opaque `Adbc.*` primitives declared in Lean's
  `Tc/Data/ADBC/Table.lean` so that the L6 port of `AdbcTable` can be a
  literal translation. Lean's ADBC namespace operates on an implicit
  in-process connection; we replicate that with an IORef holding a
  singleton `DB.Conn`.
-}
module Tv.Data.ADBC.Adbc
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
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Word (Word64)
import System.IO.Unsafe (unsafePerformIO)

import qualified Tv.Data.DuckDB as DB
import qualified Tv.Types as Tc
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Global Conn, mirroring Lean's implicit ADBC state.
{-# NOINLINE connRef #-}
connRef :: IORef (Maybe DB.Conn)
connRef = unsafePerformIO (newIORef Nothing)

getConn :: IO DB.Conn
getConn = do
  m <- readIORef connRef
  case m of
    Just c  -> pure c
    Nothing -> error "Adbc: connection not initialized (call Adbc.init first)"

-- | Query result with eagerly fetched chunks plus cached metadata. Lean's
-- opaque QueryResult wraps a C-side Arrow array cursor; we materialize the
-- DuckDB chunk list once at query time so `cellStr` can index by (row,col)
-- without re-iterating the one-shot `duckdb_fetch_chunk` cursor.
data QueryResult = QueryResult
  { qrChunks   :: V.Vector DB.DataChunk
  , qrOffsets  :: V.Vector Int          -- prescan of chunk sizes: row → chunk idx
  , qrNRows    :: Int
  , qrColNames :: V.Vector Text
  , qrColTypes :: V.Vector Tc.ColType
  }
makeFieldLabelsNoPrefix ''QueryResult

-- | Adbc.init — opens an in-memory DuckDB if none is open yet. Returns ""
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
    Left (e :: SomeException)  -> pure (T.pack (show e))

-- | Adbc.shutdown — disconnect and clear the singleton.
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
    { qrChunks   = chVec
    , qrOffsets  = offs
    , qrNRows    = nr
    , qrColNames = DB.columnNames r
    , qrColTypes = DB.columnTypes r
    }

-- | Parameterized query — Lean uses this for INSERT ... VALUES. For now it
-- ignores the param and runs sql directly. Rewire when a caller needs it.
queryParam :: Text -> Text -> IO QueryResult
queryParam sql _param = query sql

ncols :: QueryResult -> IO Word64
ncols qr = pure (fromIntegral (V.length (qrColNames qr)))

nrows :: QueryResult -> IO Word64
nrows qr = pure (fromIntegral (qrNRows qr))

colName :: QueryResult -> Word64 -> IO Text
colName qr i = pure (qrColNames qr V.! fromIntegral i)

-- | Adbc.colFmt — Lean returns the Arrow ArrowSchema.format string. DuckDB
-- doesn't surface that directly, so we synthesize the first character per
-- Arrow conventions: int = 'l', float = 'g', str = 'u', bool = 'b', and
-- 'd'/'t'/'s' for decimal/time/timestamp. The downstream consumer in
-- cbits/tv_render.c reads only the first char, and all date/time types
-- (Arrow tdD / ttu / tsu:*) must start with 't' so `type_char_fmt` maps
-- them to the '@' header indicator.
colFmt :: QueryResult -> Word64 -> IO Text
colFmt qr i =
  let ty = qrColTypes qr V.! fromIntegral i
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

-- | Adbc.colType — returns the StrEnum name of the column type. Lean
-- returns snake-case names like "int"/"float"/"utf8"; we use our ColType
-- StrEnum toString to keep a single source of truth. NOTE: Lean's Arrow
-- path returns "utf8" for strings; we return "str". AdbcTable.ofQueryResult
-- consumes this via `ColType.ofString`, which would have to know about
-- "utf8". When we port that, we'll either normalize there or patch this to
-- emit Arrow names — leaving this as-is for now so the ColType round-trip
-- stays symmetric with Tv.Types.
colType :: QueryResult -> Word64 -> IO Text
colType qr i = pure (ctToString (qrColTypes qr V.! fromIntegral i))
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
      in pure (DB.readCellAny cv local)

cellInt :: QueryResult -> Word64 -> Word64 -> IO Int64
cellInt qr r c = do
  let r' = fromIntegral r; c' = fromIntegral c
  case findChunk qr r' of
    Nothing -> pure 0
    Just (ch, local) ->
      let cv = DB.chunkColumn ch c'
      in pure (maybe 0 id (DB.readCellInt cv local))

cellFloat :: QueryResult -> Word64 -> Word64 -> IO Double
cellFloat qr r c = do
  let r' = fromIntegral r; c' = fromIntegral c
  case findChunk qr r' of
    Nothing -> pure 0
    Just (ch, local) ->
      let cv = DB.chunkColumn ch c'
      in pure (maybe 0 id (DB.readCellDouble cv local))

-- | Locate (chunk, local row index) for a global row index. Binary search
-- over the prescanned chunk offsets.
findChunk :: QueryResult -> Int -> Maybe (DB.DataChunk, Int)
findChunk qr row
  | row < 0 || row >= qrNRows qr = Nothing
  | V.null (qrChunks qr) = Nothing
  | otherwise =
      let offs = qrOffsets qr
          chs  = qrChunks qr
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

-- | Format a single cell for display. Dispatches on column type:
-- ints get comma formatting, floats use precision control, bools are true/false.
formatCellDisplay :: QueryResult -> Int -> Int -> Int -> Tc.ColType -> IO T.Text
formatCellDisplay qr row col prec_ typ =
  case findChunk qr row of
    Nothing -> pure ""
    Just (ch, local) ->
      let cv = DB.chunkColumn ch (fromIntegral col)
      in case typ of
        Tc.ColTypeInt -> pure $ case DB.readCellInt cv local of
          Nothing -> ""
          Just n  -> fmtIntComma n
        Tc.ColTypeFloat -> pure $ case DB.readCellDouble cv local of
          Nothing -> ""
          Just f  -> if isNaN f then "" else T.pack (showFFloat (Just prec_) f "")
        Tc.ColTypeDecimal -> pure $ case DB.readCellDouble cv local of
          Nothing -> ""
          Just f  -> if isNaN f then "" else T.pack (showFFloat (Just prec_) f "")
        Tc.ColTypeBool -> pure $ case DB.readCellInt cv local of
          Just 0  -> "false"
          Just _  -> "true"
          Nothing -> ""
        _ -> pure (DB.readCellAny cv local)

-- | Materialize a rectangular cell region as display-formatted text.
-- Column-major: outer Vector indexed by column, inner by local row.
fetchRows :: QueryResult -> Int -> Int -> Int -> IO (V.Vector (V.Vector T.Text))
fetchRows qr r0 r1 prec_ =
  let nc = V.length (qrColNames qr)
      types = qrColTypes qr
  in V.generateM nc $ \c ->
    V.generateM (max 0 (r1 - r0)) $ \ri ->
      formatCellDisplay qr (r0 + ri) c prec_
        (if c < V.length types then types V.! c else Tc.ColTypeOther)

-- | Fetch raw doubles for heat mode. NaN for non-numeric or null cells.
fetchHeatDoubles :: QueryResult -> Int -> Int -> IO (V.Vector (V.Vector Double))
fetchHeatDoubles qr r0 r1 =
  let nc = V.length (qrColNames qr)
      types = qrColTypes qr
      nan = 0/0 :: Double
  in V.generateM nc $ \c ->
    let typ = if c < V.length types then types V.! c else Tc.ColTypeOther
    in V.generateM (max 0 (r1 - r0)) $ \ri ->
      let row = r0 + ri
      in case findChunk qr row of
        Nothing -> pure nan
        Just (ch, local) ->
          let cv = DB.chunkColumn ch (fromIntegral c)
          in case typ of
            Tc.ColTypeInt -> pure $ case DB.readCellInt cv local of
              Nothing -> nan
              Just n  -> fromIntegral n
            Tc.ColTypeFloat -> pure $ case DB.readCellDouble cv local of
              Nothing -> nan
              Just f  -> f
            Tc.ColTypeDecimal -> pure $ case DB.readCellDouble cv local of
              Nothing -> nan
              Just f  -> f
            _ -> pure nan
