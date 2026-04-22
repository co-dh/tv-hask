{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  DuckDB table: AdbcTable with PRQL query support.

  Literal port of Tc/Data/ADBC/Table.lean. The Lean file declares its
  `namespace Adbc` externs and a top-level `Prql.query` helper, then
  collects `AdbcTable` and its builder/modifier helpers under
  `namespace Tc.AdbcTable`. Haskell has no re-openable namespaces, so
  everything lives at top level and name suffixes match Lean's: the Lean
  `AdbcTable.foo` becomes `foo` here (with prefixed aliases only where a
  collision forces it).

  NOTE: Table operations (getCols, renderView, etc.) are in DuckDB/Ops.hs.
-}
module Tv.Data.DuckDB.Table where

import Prelude hiding (init, filter)
import Tv.Prelude
import Control.Exception (SomeException, try)
import Data.IORef (atomicModifyIORef')
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Read as TR
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import System.IO.Unsafe (unsafePerformIO)

import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Prql (Query (..))
import Tv.Types
  ( ColType (..)
  , Op (..)
  , ofString
  , escSql
  , keepCols
  )
import qualified Tv.Util as Log
import qualified Tv.Util as Tmp
import Optics.TH (makeFieldLabelsNoPrefix)

-- ----------------------------------------------------------------------------
-- Top-level (Lean: `def Prql.query` in Table.lean, not Prql.lean)
-- ----------------------------------------------------------------------------

-- | Compile PRQL and execute via Conn. Logs the PRQL, returns Nothing on
-- compile failure. Named `prqlQuery` to avoid colliding with
-- Tv.Data.DuckDB.Prql.Query (the record type).
prqlQuery :: Text -> IO (Maybe Conn.QueryResult)
prqlQuery prql = do
  Log.write "prql" prql
  m <- Prql.compile prql
  case m of
    Nothing  -> pure Nothing
    Just sql -> Just <$> Conn.query sql

-- ----------------------------------------------------------------------------
-- namespace Tc
-- ----------------------------------------------------------------------------

-- | Default row limit for PRQL queries
prqlLimit :: Int
prqlLimit = 1000

-- | Trim whitespace and a trailing semicolon from compiled SQL
stripSemi :: Text -> Text
stripSemi s =
  let t = T.strip s
  in if T.isSuffixOf ";" t then T.dropEnd 1 t else t

-- | Zero-copy table with PRQL query for re-query on modification
data AdbcTable = AdbcTable
  { qr        :: Conn.QueryResult  -- arrow data (opaque, C memory)
  , colNames  :: Vector Text       -- cached column names
  , colFmts   :: Vector Char       -- cached format chars per column
  , colTypes  :: Vector ColType    -- cached type names
  , nRows     :: Int               -- rows in current result (<= prqlLimit)
  , query     :: Query             -- PRQL query (base + ops)
  , totalRows :: Int               -- total rows in underlying data
  }
makeFieldLabelsNoPrefix ''AdbcTable

-- | Counter for unique temp table names (mirrors Lean `IO.Ref Nat`).
{-# NOINLINE tblCounter #-}
tblCounter :: IORef Int
tblCounter = unsafePerformIO (newIORef 0)

-- | Allocate a unique temp table name: tc_{label}_{n}
tmpName :: Text -> IO Text
tmpName label = do
  n <- atomicModifyIORef' tblCounter (\n -> (n + 1, n))
  pure $ "tc_" <> label <> "_" <> T.pack (show n)

-- | Track loaded DuckDB extensions (idempotent install+load)
{-# NOINLINE extLoaded #-}
extLoaded :: IORef (Vector Text)
extLoaded = unsafePerformIO (newIORef V.empty)

loadExt :: Text -> IO ()
loadExt ext
  | T.null ext = pure ()
  | otherwise = do
      done <- readIORef extLoaded
      if V.elem ext done
        then pure ()
        else do
          Log.write "ext" ("loading: " <> ext)
          _ <- Conn.query ("INSTALL " <> ext <> "; LOAD " <> ext)
          modifyIORef' extLoaded (`V.snoc` ext)

-- ----------------------------------------------------------------------------
-- namespace AdbcTable
-- ----------------------------------------------------------------------------

-- | Init DuckDB backend, install+load httpfs for hf:// support.
--   Returns "" on success, error message on failure.
init :: IO Text
init = do
  err <- Conn.init
  if T.null err
    then do
      r <- try (Conn.query "INSTALL httpfs; LOAD httpfs") :: IO (Either SomeException Conn.QueryResult)
      case r of
        Left e  -> Log.write "init" ("httpfs extension: " <> T.pack (show e))
        Right _ -> pure ()
      pure err
    else pure err

-- | Shutdown DuckDB backend
shutdown :: IO ()
shutdown = Conn.shutdown

-- | Build AdbcTable from QueryResult
ofResult :: Conn.QueryResult -> Query -> Int -> IO AdbcTable
ofResult qr_ q total = do
  nc <- Conn.ncols qr_
  nr <- Conn.nrows qr_
  let ncI = fromIntegral nc :: Int
  namesMV <- MV.new ncI
  fmtsMV  <- MV.new ncI
  typesMV <- MV.new ncI
  forM_ [0 .. ncI - 1] $ \i -> do
    let iW = fromIntegral i :: Word64
    n <- Conn.colName qr_ iW
    MV.write namesMV i n
    fmt <- Conn.colFmt qr_ iW
    MV.write fmtsMV i (if not (T.null fmt) then T.head fmt else '?')
    typ <- Conn.colType qr_ iW
    MV.write typesMV i (ofString typ)
  names <- V.freeze namesMV
  fmts  <- V.freeze fmtsMV
  types <- V.freeze typesMV
  pure AdbcTable
    { qr        = qr_
    , colNames  = names
    , colFmts   = fmts
    , colTypes  = types
    , nRows     = fromIntegral nr
    , query     = q
    , totalRows = total
    }

-- | Query total row count using cnt function
queryCount :: Query -> IO Int
queryCount q = do
  m <- prqlQuery (Prql.queryRender q <> " | cnt")
  case m of
    Nothing -> pure 0
    Just qr_ -> do
      nr <- Conn.nrows qr_
      if fromIntegral nr > (0 :: Int)
        then do
          v <- Conn.cellInt qr_ 0 0
          pure $ fromIntegral v
        else pure 0

-- | Execute PRQL query and return new AdbcTable (preserves totalRows if provided)
requery :: Query -> Int -> IO (Maybe AdbcTable)
requery q total = do
  m <- prqlQuery (Prql.queryRender q <> " | take " <> T.pack (show prqlLimit))
  case m of
    Nothing  -> pure Nothing
    Just qr_ -> Just <$> ofResult qr_ q total

-- | Build AdbcTable from an existing temp table name
fromTmp :: Text -> IO (Maybe AdbcTable)
fromTmp tblName = do
  let q = Prql.defaultQuery { Prql.base = "from " <> tblName }
  total <- queryCount q
  requery q total

-- | Sanitize path to valid SQL identifier (alphanumeric + underscore)
remoteName :: Text -> Text
remoteName path_ =
  let s = T.map (\c -> if isAlphaNum c then c else '_') path_
  in "tc_" <> s
  where
    isAlphaNum c =
      (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')

-- | Create from file path (queries total count).
--   Remote URLs (hf://) are materialized into a DuckDB temp table first.
fromFile :: Text -> IO (Maybe AdbcTable)
fromFile path_ = do
  q <- if T.isPrefixOf "hf://" path_
         then do
           let tbl = remoteName path_
           _ <- Conn.query
                  ( "CREATE OR REPLACE TEMP TABLE \"" <> tbl
                 <> "\" AS (SELECT * FROM '" <> escSql path_ <> "')" )
           pure $ Prql.defaultQuery { Prql.base = "from " <> tbl }
         else
           pure $ Prql.defaultQuery { Prql.base = "from `" <> path_ <> "`" }
  total <- queryCount q
  requery q total

-- | Attach a .duckdb file and list its tables as TSV (for folder-like view)
listTables :: Text -> IO (Maybe AdbcTable)
listTables path_ = do
  _ <- Conn.query ("ATTACH '" <> escSql path_ <> "' AS extdb (READ_ONLY)")
  m <- prqlQuery Prql.ducktabs
  case m of
    Nothing -> pure Nothing
    Just qr_ -> do
      total <- Conn.nrows qr_
      if fromIntegral total == (0 :: Int)
        then pure Nothing
        else Just <$> ofResult qr_ (Prql.defaultQuery { Prql.base = Prql.ducktabs }) (fromIntegral total)

-- | Get primary key columns for a table in the attached extdb
primaryKeys :: Text -> IO (Vector Text)
primaryKeys table = do
  r <- try action :: IO (Either SomeException (Vector Text))
  case r of
    Left e  -> do
      Log.write "table" ("primaryKeys " <> table <> ": " <> T.pack (show e))
      pure V.empty
    Right v -> pure v
  where
    action = do
      m <- prqlQuery ("from dcons | prim_keys '" <> escSql table <> "'")
      case m of
        Nothing -> pure V.empty
        Just qr_ -> do
          nr <- Conn.nrows qr_
          let n = fromIntegral nr :: Int
          V.generateM n $ \i -> Conn.cellStr qr_ i 0

-- | Open a table/view from an attached .duckdb file or schema-qualified name
fromTable :: Text -> IO (Maybe (AdbcTable, Vector Text))
fromTable table = do
  keys <- primaryKeys table
  let qualName = if T.isInfixOf "." table then table else "extdb." <> table
      q = Prql.defaultQuery { Prql.base = "from " <> qualName }
  total <- queryCount q
  m <- requery q total
  pure $ fmap (\t -> (t, keys)) m

-- | Sort: append sort op and re-query (all columns use given direction)
sortBy :: AdbcTable -> Vector Int -> Bool -> IO AdbcTable
sortBy t idxs asc = do
  let sortCols = V.map (\idx -> (fromMaybe "" ((t ^. #colNames) V.!? idx), asc)) idxs
      newQuery = Prql.pipe (t ^. #query) (OpSort sortCols)
  m <- requery newQuery (t ^. #totalRows)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Hide columns: append select op and re-query
hideCols :: AdbcTable -> Vector Int -> IO AdbcTable
hideCols t hideIdxs = do
  let kept = keepCols (V.length (t ^. #colNames)) hideIdxs (t ^. #colNames)
  m <- requery (Prql.pipe (t ^. #query) (OpSel kept)) (t ^. #totalRows)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Exclude columns: append EXCLUDE op and re-query (DuckDB SELECT * EXCLUDE)
excludeCols :: AdbcTable -> Vector Text -> IO AdbcTable
excludeCols t cols = do
  m <- requery (Prql.pipe (t ^. #query) (OpExclude cols)) (t ^. #totalRows)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Fetch more rows (increase limit by prqlLimit)
fetchMore :: AdbcTable -> IO (Maybe AdbcTable)
fetchMore t
  | t ^. #nRows >= t ^. #totalRows = pure Nothing
  | otherwise = do
      let limit_ = (t ^. #nRows) + prqlLimit
      m <- prqlQuery (Prql.queryRender (t ^. #query) <> " | take " <> T.pack (show limit_))
      case m of
        Nothing  -> pure Nothing
        Just qr_ -> Just <$> ofResult qr_ (t ^. #query) (t ^. #totalRows)

-- | Build the plot PRQL pipeline: ds_trunc for time x-axis (SUBSTRING bucketing),
-- ds_nth for non-time (every-Nth-row sampling). The _cat suffix keeps a grouping
-- column through the downsample.
plotPrql :: Text -> Text -> Text -> Maybe Text -> Bool -> Int -> Text
plotPrql baseR xName yName catName_ xIsTime truncLen =
  let q = Prql.ref   -- column references (keyword-safe via this.col)
      tn = T.pack (show truncLen)
  in if xIsTime
       then case catName_ of
              Just cn -> baseR <> " | ds_trunc_cat " <> q xName <> " " <> q yName
                               <> " " <> q cn <> " " <> tn
              Nothing -> baseR <> " | ds_trunc " <> q xName <> " " <> q yName
                               <> " " <> tn
       else
         let selCols = case catName_ of
               Just cn -> q xName <> ", " <> q yName <> ", " <> q cn
               Nothing -> q xName <> ", " <> q yName
             dsFn = case catName_ of
               Just cn -> "ds_nth_cat " <> q yName <> " " <> q cn <> " " <> tn
               Nothing -> "ds_nth " <> q yName <> " " <> tn
         in baseR <> " | " <> dsFn <> " | select {" <> selCols <> "}"

-- | Per-thread plot data path. The path is stable within one thread (so
-- one plot session writes and reads the same file) but differs across
-- threads (so parallel test threads don't clobber each other).
plotDatPath :: IO FilePath
plotDatPath = Tmp.threadPath "plot.dat"

-- | Run COPY (sql) TO the thread-local plot data file as TSV. Raises
-- userError on failure so the plot pipeline fails loudly instead of
-- silently empty-plotting.
copyPlot :: Text -> IO ()
copyPlot sql = do
  let sql' = stripSemi sql
  datPath <- plotDatPath
  let copySql = "COPY (" <> sql' <> ") TO '" <> T.pack datPath
             <> "' (FORMAT CSV, DELIMITER '\t', HEADER false)"
  Log.write "plot-sql" copySql
  r <- try (Conn.query copySql) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Left e -> do
      let msg = "COPY failed: " <> T.pack (show e)
      Log.write "plot" msg
      ioError (userError (T.unpack msg))
    Right _ -> pure ()

-- | Query distinct category values (for legend/facet labels in R).
uniqCats :: Text -> Text -> IO (Vector Text)
uniqCats baseR cn = do
  m <- prqlQuery (baseR <> " | uniq " <> Prql.ref cn)
  case m of
    Nothing -> pure V.empty
    Just catQr -> do
      nr <- Conn.nrows catQr
      let n = fromIntegral nr :: Int
      V.generateM n $ \i -> Conn.cellStr catQr i 0

-- | Export plot data to the thread-local plot file via DuckDB COPY
-- (downsample in SQL). truncLen: SUBSTRING length for time truncation;
-- _step: every-Nth-row for non-time. Callers read back the same path via
-- 'plotDatPath'.
plotExport
  :: AdbcTable
  -> Text          -- xName
  -> Text          -- yName
  -> Maybe Text    -- catName?
  -> Bool          -- xIsTime
  -> Int           -- _step (unused, matches Lean signature)
  -> Int           -- truncLen
  -> IO (Maybe (Vector Text))
plotExport t xName yName catName_ xIsTime _step truncLen = do
  let baseR = Prql.queryRender (t ^. #query)
      prqlStr = plotPrql baseR xName yName catName_ xIsTime truncLen
  Log.write "prql" prqlStr
  mSql <- Prql.compile prqlStr
  case mSql of
    Nothing -> pure Nothing
    Just sql -> do
      copyPlot sql
      case catName_ of
        Just cn -> Just <$> uniqCats baseR cn
        Nothing -> pure (Just V.empty)

-- | Export 5-column TSV (x, open, high, low, close) for candlestick R rendering.
-- Output columns are renamed to literal 'open'/'high'/'low'/'close' so the
-- R template is agnostic to the source column names.
plotExportOhlc
  :: AdbcTable
  -> Text   -- xName
  -> Text   -- openName
  -> Text   -- highName
  -> Text   -- lowName
  -> Text   -- closeName
  -> IO Bool
plotExportOhlc t xName openName highName lowName closeName = do
  let baseR = Prql.queryRender (t ^. #query)
      q     = Prql.ref
      prqlStr = baseR
        <> " | select { " <> q xName
        <> ", open = " <> q openName
        <> ", high = " <> q highName
        <> ", low = "  <> q lowName
        <> ", close = " <> q closeName
        <> " }"
  Log.write "prql" prqlStr
  mSql <- Prql.compile prqlStr
  case mSql of
    Nothing -> pure False
    Just sql -> do
      copyPlot sql
      pure True

-- | Ingest content via DuckDB reader into a temp table (private helper)
fromIngest :: Text -> Text -> Text -> IO (Maybe AdbcTable)
fromIngest content label reader
  | T.null content || T.strip content == "[]" = pure Nothing
  | otherwise = do
      n <- atomicModifyIORef' tblCounter (\x -> (x + 1, x))
      tmp <- Tmp.tmpPath (T.unpack label <> "-" <> show n <> "." <> T.unpack label)
      TIO.writeFile tmp content
      let tbl = "tc_" <> label <> "_" <> T.pack (show n)
      r <- try (Conn.query ("CREATE TEMP TABLE " <> tbl
                          <> " AS SELECT * FROM " <> reader
                          <> "('" <> T.pack tmp <> "')"))
           :: IO (Either SomeException Conn.QueryResult)
      case r of
        Left e -> do
          Tmp.rmFile tmp
          Log.write label ("error: " <> T.pack (show e))
          pure Nothing
        Right _ -> do
          Tmp.rmFile tmp
          let q = Prql.defaultQuery { Prql.base = "from " <> tbl }
          total <- queryCount q
          requery q total

fromTsv :: Text -> IO (Maybe AdbcTable)
fromTsv content = fromIngest content "tsv" "read_csv_auto"

fromJson :: Text -> IO (Maybe AdbcTable)
fromJson content = fromIngest content "json" "read_json_auto"

-- | Create from file path with optional setup SQL and reader function.
--   reader: DuckDB reader function (e.g. "read_arrow"). Empty = auto-detect via backtick.
--   duckdbExt: extension to install+load first (may be empty).
fileWith :: Text -> Text -> Text -> IO (Maybe AdbcTable)
fileWith path_ reader duckdbExt = do
  loadExt duckdbExt
  if T.null reader
    then fromFile path_
    else do
      -- Materialize via reader function into temp table (PRQL can't parse
      -- function calls in `from`).
      let tbl = remoteName path_
      _ <- Conn.query
             ( "CREATE OR REPLACE TEMP TABLE \"" <> tbl
            <> "\" AS (SELECT * FROM " <> reader
            <> "('" <> escSql path_ <> "'))" )
      let q = Prql.defaultQuery { Prql.base = "from " <> tbl }
      total <- queryCount q
      requery q total

-- ----------------------------------------------------------------------------
-- NOTE: Table operations (getCols, renderView, etc.) are in DuckDB/Ops.hs
-- ----------------------------------------------------------------------------

-- | Create freq table entirely in SQL (no round-trip to Lean)
freqTable :: AdbcTable -> Vector Text -> IO (Maybe (AdbcTable, Int))
freqTable t cNames
  | V.null cNames = pure Nothing
  | otherwise = do
      let cols = T.intercalate ", " (V.toList (V.map Prql.ref cNames))
          baseR = Prql.queryRender (t ^. #query)
      -- total distinct groups
      totalGroups <- do
        m <- prqlQuery (baseR <> " | cntdist {" <> cols <> "}")
        case m of
          Nothing -> pure 0
          Just qr_ -> do
            v <- Conn.cellStr qr_ 0 0
            pure $ parseIntOr0 v
      -- freq table: uses freq PRQL function which computes Cnt, Pct, Bar in SQL
      tblName <- tmpName "freq"
      let prql = baseR <> " | freq {" <> cols <> "} | take 1000"
      Log.write "prql" prql
      mSql <- Prql.compile prql
      case mSql of
        Nothing -> pure Nothing
        Just sql -> do
          let sql' = stripSemi sql
          _ <- Conn.query ("CREATE TEMP TABLE " <> tblName <> " AS " <> sql')
          m <- requery (Prql.defaultQuery { Prql.base = "from " <> tblName }) 0
          case m of
            Just t' -> pure (Just (t', totalGroups))
            Nothing -> pure Nothing
  where
    parseIntOr0 s = case TR.decimal s of
      Right (n, _) -> n
      Left _       -> 0

-- | Filter: requery with filter (queries new filtered count)
filter :: AdbcTable -> Text -> IO (Maybe AdbcTable)
filter t expr = do
  let q = Prql.queryFilter (t ^. #query) expr
  total <- queryCount q
  requery q total

-- | Distinct: use SQL DISTINCT
distinct :: AdbcTable -> Int -> IO (Vector Text)
distinct t col = do
  let cName = fromMaybe "" ((t ^. #colNames) V.!? col)
  m <- prqlQuery (Prql.queryRender (t ^. #query) <> " | uniq " <> Prql.ref cName)
  case m of
    Nothing -> pure V.empty
    Just qr_ -> do
      nr <- Conn.nrows qr_
      let n = fromIntegral nr :: Int
      V.generateM n $ \i -> Conn.cellStr qr_ i 0

-- | Find row from starting position, forward or backward (with wrap).
--   PRQL row_number is 1-based; we subtract 1 here for 0-based indexing.
findRow :: AdbcTable -> Int -> Text -> Int -> Bool -> IO (Maybe Int)
findRow t col val start fwd = do
  let cName = Prql.ref (fromMaybe "" ((t ^. #colNames) V.!? col))
      prql  = Prql.queryRender (t ^. #query)
           <> " | derive {_rn = row_number this} | filter ("
           <> cName <> " == '" <> escSql val <> "') | select {_rn}"
  m <- prqlQuery prql
  case m of
    Nothing  -> pure Nothing
    Just qr_ -> do
      nr <- Conn.nrows qr_
      let n = fromIntegral nr :: Int
      rows <- V.generateM n $ \i -> do
        v <- Conn.cellInt qr_ i 0
        pure (fromIntegral v - 1 :: Int)
      if V.null rows
        then pure Nothing
        else if fwd
               then case V.find (>= start) rows of
                      Just r  -> pure (Just r)
                      Nothing -> pure $ Just $ V.head rows
               else
                 -- findRev?: last element satisfying (< start), else fall back to back
                 let revHit = V.foldl'
                                (\acc r -> if r < start then Just r else acc)
                                Nothing rows
                 in case revHit of
                      Just r  -> pure (Just r)
                      Nothing -> pure $ Just $ V.last rows

