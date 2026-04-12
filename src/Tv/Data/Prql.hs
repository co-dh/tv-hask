{-# LANGUAGE OverloadedStrings #-}
-- | PRQL query building and compilation via prqlc subprocess.
-- Faithful translation of Tc/Tc/Data/ADBC/Prql.lean.
module Tv.Data.Prql
  ( Query(..), mkQuery
  , quote, renderSort, aggName, dqQuote
  , renderOp, renderOps, render
  , pipe, pfilter
  , ducktabs, ducktabsF
  , funcs
  , compile
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Control.Exception (try, SomeException)
import System.IO (hClose, hFlush, hGetContents, hPutStr, hPutStrLn, stderr)
import System.Process (CreateProcess(..), StdStream(..), createProcess, proc, waitForProcess)
import System.Exit (ExitCode(..))
import Tv.Types (Agg(..), Op(..))

-- | PRQL query: base table + pipeline of operations
data Query = Query { base :: !Text, ops :: !(Vector Op) } deriving (Eq, Show)

mkQuery :: Query
mkQuery = Query "from df" V.empty

-- | Backtick-quote a column name to avoid PRQL keyword collisions
quote :: Text -> Text
quote s = "`" <> s <> "`"

-- | Render sort column: ascending = col, descending = -col
renderSort :: Text -> Bool -> Text
renderSort col asc = let qc = quote col in if asc then qc else "-" <> qc

-- | PRQL aggregate function name (std. prefix)
aggName :: Agg -> Text
aggName ACount = "std.count"; aggName ASum = "std.sum"; aggName AAvg = "std.average"
aggName AMin = "std.min"; aggName AMax = "std.max"; aggName AStddev = "std.stddev"
aggName ADist = "std.count_distinct"

-- | DuckDB-quote for use inside PRQL s-strings (\" escapes)
dqQuote :: Text -> Text
dqQuote s = "\\\"" <> s <> "\\\""

-- | Render a single Op to a PRQL pipeline step
renderOp :: Op -> Text
renderOp (OpFilter e) = "filter " <> e
renderOp (OpSort cols) =
  "sort {" <> T.intercalate ", " (map (\(c, asc) -> renderSort c asc) (V.toList cols)) <> "}"
renderOp (OpSel cols) =
  "select {" <> T.intercalate ", " (map quote (V.toList cols)) <> "}"
renderOp (OpExclude cols) =
  "select s\"* EXCLUDE (" <> T.intercalate ", " (map dqQuote (V.toList cols)) <> ")\""
renderOp (OpDerive bs) =
  "derive {" <> T.intercalate ", " (map (\(n, e) -> quote n <> " = " <> e) (V.toList bs)) <> "}"
renderOp (OpGroup keys aggs) =
  let as = map (\(fn, name, col) -> name <> " = " <> aggName fn <> " " <> quote col) (V.toList aggs)
  in "group {" <> T.intercalate ", " (map quote (V.toList keys))
     <> "} (aggregate {" <> T.intercalate ", " as <> "})"
renderOp (OpTake n) = "take " <> T.pack (show n)

-- | Render ops portion only (no base/from clause)
renderOps :: Query -> Text
renderOps q | V.null (ops q) = ""
            | otherwise = T.intercalate " | " (map renderOp (V.toList (ops q)))

-- | Render full PRQL query string
render :: Query -> Text
render q = let o = renderOps q
           in if T.null o then base q else base q <> " | " <> o

-- | Append an operation to the query pipeline
pipe :: Query -> Op -> Query
pipe q op = q { ops = V.snoc (ops q) op }

-- | Filter helper
pfilter :: Query -> Text -> Query
pfilter q expr = pipe q (OpFilter expr)

-- Common PRQL query strings
ducktabs, ducktabsF :: Text
ducktabs  = "from dtabs | tbl_info"
ducktabsF = "from dtabs | tbl_info_filtered"

-- | PRQL function definitions (compile-time embed equivalent).
-- Embedded as a Haskell string literal matching funcs.prql content.
funcs :: Text
funcs = T.unlines
  [ "# PRQL function definitions for tc"
  , "let dtabs = s\"SELECT * FROM duckdb_tables()\""
  , "let dcols = s\"SELECT * FROM duckdb_columns()\""
  , "let dcons = s\"SELECT * FROM duckdb_constraints()\""
  , ""
  , "let freq = func c tbl <relation> -> ("
  , "  from tbl"
  , "  group {c} (aggregate {Cnt = std.count this})"
  , "  derive {Pct = Cnt * 100 / std.sum Cnt, Bar = s\"repeat('#', CAST({Pct}/5 AS INTEGER))\"}"
  , "  sort {-Cnt}"
  , ")"
  , ""
  , "let cnt = func tbl <relation> -> (from tbl | aggregate {n = std.count this})"
  , ""
  , "let cntdist = func cols tbl <relation> -> (from tbl | group cols (take 1) | aggregate {n = std.count this})"
  , ""
  , "let uniq = func c tbl <relation> -> (from tbl | group {c} (take 1) | select {c})"
  , ""
  , "let stats = func c tbl <relation> -> ("
  , "  from tbl"
  , "  aggregate {n = std.count this, min = std.min c, max = std.max c, avg = std.average c, std = std.stddev c}"
  , ")"
  , ""
  , "let meta = func c tbl <relation> -> ("
  , "  from tbl"
  , "  aggregate {cnt = std.count c, dist = std.count_distinct c, total = std.count this, min = std.min c, max = std.max c}"
  , ")"
  , ""
  , "let ds_trunc = func x y len tbl <relation> -> ("
  , "  from tbl"
  , "  filter (y != null)"
  , "  derive {_sec = s\"SUBSTRING(CAST({x} AS VARCHAR), 1, {len})\"}"
  , "  group {_sec} (sort {-x} | take 1)"
  , "  sort {_sec}"
  , "  select {_sec, y}"
  , ")"
  , ""
  , "let ds_trunc_cat = func x y c len tbl <relation> -> ("
  , "  from tbl"
  , "  filter (y != null)"
  , "  derive {_sec = s\"SUBSTRING(CAST({x} AS VARCHAR), 1, {len})\"}"
  , "  group {_sec, c} (sort {-x} | take 1)"
  , "  sort {_sec}"
  , "  select {_sec, y, c}"
  , ")"
  , ""
  , "let ds_nth = func y step tbl <relation> -> ("
  , "  from tbl"
  , "  filter (y != null)"
  , "  derive {_rn = row_number this}"
  , "  filter (_rn - 1) % step == 0"
  , ")"
  , ""
  , "let ds_nth_cat = func y c step tbl <relation> -> ("
  , "  from tbl"
  , "  filter (y != null)"
  , "  derive {_rn = row_number this}"
  , "  filter (_rn - 1) % step == 0"
  , ")"
  , ""
  , "let rowidx = func tbl <relation> -> ("
  , "  from tbl"
  , "  derive {idx = s\"(ROW_NUMBER() OVER () - 1)\"}"
  , ")"
  , ""
  , "let pqmeta = func tbl <relation> -> ("
  , "  from tbl"
  , "  filter path_in_schema != ''"
  , "  group {path_in_schema, `type`} ("
  , "    aggregate {"
  , "      _cnt = sum num_values, _null = sum stats_null_count,"
  , "      mn = min stats_min_value, mx = max stats_max_value,"
  , "      _id = min column_id"
  , "    }"
  , "  )"
  , "  sort {_id}"
  , "  derive {"
  , "    cnt = s\"CAST({_cnt} AS BIGINT)\","
  , "    dist = s\"0::BIGINT\","
  , "    null_pct = s\"CAST(ROUND(CAST({_null} AS FLOAT) / NULLIF({_cnt},0) * 100) AS BIGINT)\""
  , "  }"
  , "  select {column = path_in_schema, coltype = `type`, this.cnt, this.dist, this.null_pct, mn, mx}"
  , ")"
  , ""
  , "let tbl_info = func tbl <relation> -> ("
  , "  from tbl"
  , "  filter database_name == 'extdb'"
  , "  select {name = table_name, size = estimated_size, columns = column_count}"
  , ")"
  , ""
  , "let tbl_info_filtered = func tbl <relation> -> ("
  , "  from tbl"
  , "  filter s\"schema_name NOT IN ('information_schema', 'pg_catalog')\""
  , "  tbl_info"
  , ")"
  , ""
  , "let col_comment = func tbl_name col_name tbl <relation> -> ("
  , "  from tbl"
  , "  filter table_name == tbl_name"
  , "  filter column_name == col_name"
  , "  filter comment != null"
  , "  take 1"
  , "  select {comment}"
  , ")"
  , ""
  , "let prim_keys = func tname tbl <relation> -> ("
  , "  from tbl"
  , "  filter database_name == 'extdb'"
  , "  filter table_name == tname"
  , "  filter constraint_type == 'PRIMARY KEY'"
  , "  derive {col = s\"unnest(constraint_column_names)\"}"
  , "  select {col}"
  , ")"
  , ""
  , "let struct_col = func tname tbl <relation> -> ("
  , "  from tbl"
  , "  filter table_name == tname"
  , "  filter s\"data_type LIKE 'STRUCT%[]'\""
  , "  take 1"
  , "  select {column_name}"
  , ")"
  , ""
  , "let colstat = func c nm tp tbl <relation> -> ("
  , "  from tbl"
  , "  aggregate {"
  , "    cnt = s\"CAST(COUNT({c}) AS BIGINT)\","
  , "    dist = s\"CAST(COUNT(DISTINCT {c}) AS BIGINT)\","
  , "    null_pct = s\"CAST(ROUND((1.0 - COUNT({c})::FLOAT / NULLIF(COUNT(*),0)) * 100) AS BIGINT)\","
  , "    mn = s\"CAST(MIN({c}) AS VARCHAR)\","
  , "    mx = s\"CAST(MAX({c}) AS VARCHAR)\""
  , "  }"
  , "  derive {column = nm, coltype = tp}"
  , "  select {column, coltype, this.cnt, this.dist, null_pct, mn, mx}"
  , ")"
  ]

-- | Compile PRQL to SQL via prqlc CLI. Returns Nothing on error.
compile :: Text -> IO (Maybe Text)
compile prql = do
  let full = funcs <> "\n" <> prql
  r <- try $ do
    let cp = (proc "prqlc" ["compile", "--hide-signature-comment", "-t", "sql.duckdb"])
               { std_in = CreatePipe, std_out = CreatePipe, std_err = CreatePipe }
    (Just hin, Just hout, Just herr, ph) <- createProcess cp
    hPutStr hin (T.unpack full)
    hFlush hin
    hClose hin
    out <- T.pack <$> readAll hout
    err <- T.pack <$> readAll herr
    ec <- waitForProcess ph
    case ec of
      ExitSuccess   -> pure (Just out)
      ExitFailure _ -> hPutStrLn stderr ("prqlc: " <> T.unpack err) >> pure Nothing
  pure $ case r of
    Right v -> v
    Left (_ :: SomeException) -> Nothing
  where
    readAll h = do s <- hGetContents h; length s `seq` pure s
