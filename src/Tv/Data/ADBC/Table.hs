{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-
  ADBC backend: AdbcTable with PRQL query support.

  Literal port of Tc/Data/ADBC/Table.lean. The Lean file declares its
  `namespace Adbc` externs and a top-level `Prql.query` helper, then
  collects `AdbcTable` and its builder/modifier helpers under
  `namespace Tc.AdbcTable`. Haskell has no re-openable namespaces, so
  everything lives at top level and name suffixes match Lean's: the Lean
  `AdbcTable.foo` becomes `foo` here (with prefixed aliases only where a
  collision forces it).

  NOTE: Table operations (getCols, renderView, etc.) are in ADBC/Ops.hs.
-}
module Tv.Data.ADBC.Table
  ( -- top-level (Lean: def Prql.query in Table.lean)
    prqlQuery
    -- Tc namespace helpers
  , prqlLimit
  , stripSemi
    -- AdbcTable record + counters
  , AdbcTable (..)
  , tblCounter
  , tmpName
  , extLoaded
  , loadExt
    -- AdbcTable namespace: lifecycle + builders
  , init
  , shutdown
  , ofResult
  , queryCount
  , requery
  , fromTmp
  , remoteName
  , fromFile
  , listTables
  , primaryKeys
  , fromTable
  , sortBy
  , hideCols
  , excludeCols
  , fetchMore
  , plotExport
  , fromTsv
  , fromJson
  , fileWith
    -- second AdbcTable block in Lean
  , freqTable
  , filter
  , distinct
  , findRow
  ) where

import Prelude hiding (init, filter)
import qualified Prelude
import Control.Exception (SomeException, try)
import Control.Monad (forM_, when, unless)
import Data.IORef (IORef, newIORef, readIORef, writeIORef, atomicModifyIORef', modifyIORef')
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Read as TR
import Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as MV
import Data.Word (Word64)
import System.IO.Unsafe (unsafePerformIO)

import qualified Tv.Data.ADBC.Adbc as Adbc
import qualified Tv.Data.ADBC.Prql as Prql
import Tv.Data.ADBC.Prql (Query (..))
import Tv.Types
  ( ColType (..)
  , Op (..)
  , ofString
  , escSql
  , keepCols
  )
import qualified Tv.Util as Log
import Optics.Core ((&), (.~))
import Optics.TH (makeFieldLabelsNoPrefix)

-- ----------------------------------------------------------------------------
-- Top-level (Lean: `def Prql.query` in Table.lean, not Prql.lean)
-- ----------------------------------------------------------------------------

-- | Compile PRQL and execute via Adbc. Logs the PRQL, returns Nothing on
-- compile failure. Named `prqlQuery` to avoid colliding with
-- Tv.Data.ADBC.Prql.Query (the record type).
prqlQuery :: Text -> IO (Maybe Adbc.QueryResult)
prqlQuery prql = do
  Log.write "prql" prql
  m <- Prql.compile prql
  case m of
    Nothing  -> pure Nothing
    Just sql -> Just <$> Adbc.query sql

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
  { qr        :: Adbc.QueryResult  -- arrow data (opaque, C memory)
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
  pure ("tc_" <> label <> "_" <> T.pack (show n))

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
          _ <- Adbc.query ("INSTALL " <> ext <> "; LOAD " <> ext)
          modifyIORef' extLoaded (`V.snoc` ext)

-- ----------------------------------------------------------------------------
-- namespace AdbcTable
-- ----------------------------------------------------------------------------

-- | Init ADBC backend (DuckDB), install+load httpfs for hf:// support.
--   Returns "" on success, error message on failure.
init :: IO Text
init = do
  err <- Adbc.init
  if T.null err
    then do
      r <- try (Adbc.query "INSTALL httpfs; LOAD httpfs") :: IO (Either SomeException Adbc.QueryResult)
      case r of
        Left e  -> Log.write "init" ("httpfs extension: " <> T.pack (show e))
        Right _ -> pure ()
      pure err
    else pure err

-- | Shutdown ADBC backend
shutdown :: IO ()
shutdown = Adbc.shutdown

-- | Build AdbcTable from QueryResult
ofResult :: Adbc.QueryResult -> Query -> Int -> IO AdbcTable
ofResult qr_ q total = do
  nc <- Adbc.ncols qr_
  nr <- Adbc.nrows qr_
  let ncI = fromIntegral nc :: Int
  namesMV <- MV.new ncI
  fmtsMV  <- MV.new ncI
  typesMV <- MV.new ncI
  forM_ [0 .. ncI - 1] $ \i -> do
    let iW = fromIntegral i :: Word64
    n <- Adbc.colName qr_ iW
    MV.write namesMV i n
    fmt <- Adbc.colFmt qr_ iW
    MV.write fmtsMV i (if not (T.null fmt) then T.head fmt else '?')
    typ <- Adbc.colType qr_ iW
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
      nr <- Adbc.nrows qr_
      if fromIntegral nr > (0 :: Int)
        then do
          v <- Adbc.cellInt qr_ 0 0
          pure (fromIntegral v)
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
           _ <- Adbc.query
                  ( "CREATE OR REPLACE TEMP TABLE \"" <> tbl
                 <> "\" AS (SELECT * FROM '" <> escSql path_ <> "')" )
           pure (Prql.defaultQuery { Prql.base = "from " <> tbl })
         else
           pure (Prql.defaultQuery { Prql.base = "from `" <> path_ <> "`" })
  total <- queryCount q
  requery q total

-- | Attach a .duckdb file and list its tables as TSV (for folder-like view)
listTables :: Text -> IO (Maybe AdbcTable)
listTables path_ = do
  _ <- Adbc.query ("ATTACH '" <> escSql path_ <> "' AS extdb (READ_ONLY)")
  m <- prqlQuery Prql.ducktabs
  case m of
    Nothing -> pure Nothing
    Just qr_ -> do
      total <- Adbc.nrows qr_
      if fromIntegral total == (0 :: Int)
        then pure Nothing
        else Just <$> ofResult qr_ (Prql.defaultQuery { Prql.base = Prql.ducktabs }) (fromIntegral total)

-- | Get primary key columns for a table in the attached extdb
primaryKeys :: Text -> IO (Vector Text)
primaryKeys table = do
  r <- try action :: IO (Either SomeException (Vector Text))
  case r of
    Left _  -> pure V.empty
    Right v -> pure v
  where
    action = do
      m <- prqlQuery ("from dcons | prim_keys '" <> escSql table <> "'")
      case m of
        Nothing -> pure V.empty
        Just qr_ -> do
          nr <- Adbc.nrows qr_
          let n = fromIntegral nr :: Int
          V.generateM n $ \i -> Adbc.cellStr qr_ (fromIntegral i) 0

-- | Open a table/view from an attached .duckdb file or schema-qualified name
fromTable :: Text -> IO (Maybe (AdbcTable, Vector Text))
fromTable table = do
  keys <- primaryKeys table
  let qualName = if T.isInfixOf "." table then table else "extdb." <> table
      q = Prql.defaultQuery { Prql.base = "from " <> qualName }
  total <- queryCount q
  m <- requery q total
  pure (fmap (\t -> (t, keys)) m)

-- | Sort: append sort op and re-query (all columns use given direction)
sortBy :: AdbcTable -> Vector Int -> Bool -> IO AdbcTable
sortBy t idxs asc = do
  let sortCols = V.map (\idx -> (fromMaybe "" (colNames t V.!? idx), asc)) idxs
      newQuery = Prql.pipe (query t) (OpSort sortCols)
  m <- requery newQuery (totalRows t)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Hide columns: append select op and re-query
hideCols :: AdbcTable -> Vector Int -> IO AdbcTable
hideCols t hideIdxs = do
  let kept = keepCols (V.length (colNames t)) hideIdxs (colNames t)
  m <- requery (Prql.pipe (query t) (OpSel kept)) (totalRows t)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Exclude columns: append EXCLUDE op and re-query (DuckDB SELECT * EXCLUDE)
excludeCols :: AdbcTable -> Vector Text -> IO AdbcTable
excludeCols t cols = do
  m <- requery (Prql.pipe (query t) (OpExclude cols)) (totalRows t)
  case m of
    Just t' -> pure t'
    Nothing -> pure t

-- | Fetch more rows (increase limit by prqlLimit)
fetchMore :: AdbcTable -> IO (Maybe AdbcTable)
fetchMore t
  | nRows t >= totalRows t = pure Nothing
  | otherwise = do
      let limit_ = nRows t + prqlLimit
      m <- prqlQuery (Prql.queryRender (query t) <> " | take " <> T.pack (show limit_))
      case m of
        Nothing  -> pure Nothing
        Just qr_ -> Just <$> ofResult qr_ (query t) (totalRows t)

-- | Export plot data to tmpdir/plot.dat via DuckDB COPY (downsample in SQL).
-- truncLen: SUBSTRING length for time truncation; _step: every-Nth-row for non-time.
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
  let q = Prql.quote
      baseR = Prql.queryRender (query t)
      prqlStr =
        if xIsTime
          then case catName_ of
                 Just cn -> baseR <> " | ds_trunc_cat " <> q xName <> " " <> q yName
                                  <> " " <> q cn <> " " <> T.pack (show truncLen)
                 Nothing -> baseR <> " | ds_trunc " <> q xName <> " " <> q yName
                                  <> " " <> T.pack (show truncLen)
          else
            let selCols = case catName_ of
                  Just cn -> q xName <> ", " <> q yName <> ", " <> q cn
                  Nothing -> q xName <> ", " <> q yName
                dsFn = case catName_ of
                  Just cn -> "ds_nth_cat " <> q yName <> " " <> q cn
                              <> " " <> T.pack (show truncLen)
                  Nothing -> "ds_nth " <> q yName <> " " <> T.pack (show truncLen)
            in baseR <> " | " <> dsFn <> " | select {" <> selCols <> "}"
  Log.write "prql" prqlStr
  mSql <- Prql.compile prqlStr
  case mSql of
    Nothing -> pure Nothing
    Just sql -> do
      let sql' = stripSemi sql
      datPath <- Log.tmpPath "plot.dat"
      let copySql = "COPY (" <> sql' <> ") TO '" <> T.pack datPath
                 <> "' (FORMAT CSV, DELIMITER '\t', HEADER false)"
      Log.write "plot-sql" copySql
      r <- try (Adbc.query copySql) :: IO (Either SomeException Adbc.QueryResult)
      case r of
        Left e -> do
          let msg = "COPY failed: " <> T.pack (show e)
          Log.write "plot" msg
          ioError (userError (T.unpack msg))
        Right _ -> pure ()
      case catName_ of
        Just cn -> do
          m <- prqlQuery (baseR <> " | uniq " <> q cn)
          case m of
            Nothing -> pure (Just V.empty)
            Just catQr -> do
              nr <- Adbc.nrows catQr
              let n = fromIntegral nr :: Int
              cats <- V.generateM n $ \i -> Adbc.cellStr catQr (fromIntegral i) 0
              pure (Just cats)
        Nothing -> pure (Just V.empty)

-- | Ingest content via DuckDB reader into a temp table (private helper)
fromIngest :: Text -> Text -> Text -> IO (Maybe AdbcTable)
fromIngest content label reader
  | T.null content || T.strip content == "[]" = pure Nothing
  | otherwise = do
      n <- atomicModifyIORef' tblCounter (\x -> (x + 1, x))
      tmp <- Log.tmpPath (T.unpack label <> "-" <> show n <> "." <> T.unpack label)
      TIO.writeFile tmp content
      let tbl = "tc_" <> label <> "_" <> T.pack (show n)
      r <- try (Adbc.query ("CREATE TEMP TABLE " <> tbl
                          <> " AS SELECT * FROM " <> reader
                          <> "('" <> T.pack tmp <> "')"))
           :: IO (Either SomeException Adbc.QueryResult)
      case r of
        Left e -> do
          Log.rmFile tmp
          Log.write label ("error: " <> T.pack (show e))
          pure Nothing
        Right _ -> do
          Log.rmFile tmp
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
      _ <- Adbc.query
             ( "CREATE OR REPLACE TEMP TABLE \"" <> tbl
            <> "\" AS (SELECT * FROM " <> reader
            <> "('" <> escSql path_ <> "'))" )
      let q = Prql.defaultQuery { Prql.base = "from " <> tbl }
      total <- queryCount q
      requery q total

-- ----------------------------------------------------------------------------
-- NOTE: Table operations (getCols, renderView, etc.) are in ADBC/Ops.hs
-- ----------------------------------------------------------------------------

-- | Create freq table entirely in SQL (no round-trip to Lean)
freqTable :: AdbcTable -> Vector Text -> IO (Maybe (AdbcTable, Int))
freqTable t cNames
  | V.null cNames = pure Nothing
  | otherwise = do
      let cols = T.intercalate ", " (V.toList (V.map Prql.quote cNames))
          baseR = Prql.queryRender (query t)
      -- total distinct groups
      totalGroups <- do
        m <- prqlQuery (baseR <> " | cntdist {" <> cols <> "}")
        case m of
          Nothing -> pure 0
          Just qr_ -> do
            v <- Adbc.cellStr qr_ 0 0
            pure (parseIntOr0 v)
      -- freq table: uses freq PRQL function which computes Cnt, Pct, Bar in SQL
      tblName <- tmpName "freq"
      let prql = baseR <> " | freq {" <> cols <> "} | take 1000"
      Log.write "prql" prql
      mSql <- Prql.compile prql
      case mSql of
        Nothing -> pure Nothing
        Just sql -> do
          let sql' = stripSemi sql
          _ <- Adbc.query ("CREATE TEMP TABLE " <> tblName <> " AS " <> sql')
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
  let q = Prql.queryFilter (query t) expr
  total <- queryCount q
  requery q total

-- | Distinct: use SQL DISTINCT
distinct :: AdbcTable -> Int -> IO (Vector Text)
distinct t col = do
  let cName = fromMaybe "" (colNames t V.!? col)
  m <- prqlQuery (Prql.queryRender (query t) <> " | uniq " <> Prql.quote cName)
  case m of
    Nothing -> pure V.empty
    Just qr_ -> do
      nr <- Adbc.nrows qr_
      let n = fromIntegral nr :: Int
      V.generateM n $ \i -> Adbc.cellStr qr_ (fromIntegral i) 0

-- | Find row from starting position, forward or backward (with wrap).
--   PRQL row_number is 1-based; we subtract 1 here for 0-based indexing.
findRow :: AdbcTable -> Int -> Text -> Int -> Bool -> IO (Maybe Int)
findRow t col val start fwd = do
  let cName = Prql.quote (fromMaybe "" (colNames t V.!? col))
      prql  = Prql.queryRender (query t)
           <> " | derive {_rn = row_number this} | filter ("
           <> cName <> " == '" <> escSql val <> "') | select {_rn}"
  m <- prqlQuery prql
  case m of
    Nothing  -> pure Nothing
    Just qr_ -> do
      nr <- Adbc.nrows qr_
      let n = fromIntegral nr :: Int
      rows <- V.generateM n $ \i -> do
        v <- Adbc.cellInt qr_ (fromIntegral i) 0
        pure (fromIntegral v - 1 :: Int)
      if V.null rows
        then pure Nothing
        else if fwd
               then case V.find (>= start) rows of
                      Just r  -> pure (Just r)
                      Nothing -> pure (Just (V.head rows))
               else
                 -- findRev?: last element satisfying (< start), else fall back to back
                 let revHit = V.foldl'
                                (\acc r -> if r < start then Just r else acc)
                                Nothing rows
                 in case revHit of
                      Just r  -> pure (Just r)
                      Nothing -> pure (Just (V.last rows))

