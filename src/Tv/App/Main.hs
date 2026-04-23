-- App/Main: CLI parsing, app init, entry point
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.App.Main
  ( CliArgs(..)
  , parseArgs
  , runApp
  , runTsv
  , outputTable
  , appMain
  , main
  ) where

import Tv.Prelude
import Control.Exception (SomeException, fromException, try)
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (doesDirectoryExist, doesFileExist)
import System.Environment (getArgs, lookupEnv)
import System.Exit (ExitCode(..), exitSuccess, exitWith)
import System.IO (stderr, stdout, hSetBuffering, BufferMode(..))

import qualified Tv.App.Common as Common
import Tv.App.Types (AppState(..))
import Tv.Types (ColCache(..), StrEnum(toString), ViewKind(..), escSql)
import qualified Tv.Data.DuckDB.Conn as Conn
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Prql as Prql
import Tv.Data.DuckDB.Prql (funcsBytes)
import qualified Tv.Data.DuckDB.Table as AdbcTable
import Tv.Data.DuckDB.Table (AdbcTable, stripSemi)
import qualified Tv.Data.Text as TextParse
import qualified Tv.FileFormat as FileFormat
import qualified Tv.Folder as Folder
import qualified Tv.Key as Key
import qualified Tv.Render as Render
import qualified Tv.Session as Session
import qualified Tv.Source as Source
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import Tv.View (View, ViewStack(..))
import qualified Tv.View as View
import qualified Tv.Util as Log
import qualified Tv.Socket as Socket
import qualified Tv.Util as Tmp
import Optics.TH (makeFieldLabelsNoPrefix)

-- parsed CLI arguments
data CliArgs = CliArgs
  { path    :: Maybe Text
  , keys    :: Vector Text
  , test    :: Bool
  , noSign  :: Bool
  , session :: Maybe Text   -- -s "name" session restore
  , prql    :: Maybe Text   -- -p "PRQL ops" applied as initial pipeline
  , script  :: Maybe Text   -- -f FILE  read PRQL from file (multi-line, may have `let`s)
  }
makeFieldLabelsNoPrefix ''CliArgs

defArgs :: CliArgs
defArgs = CliArgs { path = Nothing, keys = V.empty, test = False, noSign = False
                  , session = Nothing, prql = Nothing, script = Nothing }

-- extract flag with value, return (value?, remaining args)
extractFlag :: Text -> [Text] -> (Maybe Text, [Text])
extractFlag flag (f : v : rest)
  | f == flag = (Just v, rest)
  | otherwise = let (r, rest') = extractFlag flag (v : rest) in (r, f : rest')
extractFlag _ other = (Nothing, other)

-- parse args: path?, -c keys?, test mode, +n, -s session, -p PRQL, -f FILE.
-- If the resolved PRQL starts with `from \`<path>\`` and no positional
-- path is given, the path is extracted from the from-clause so users
-- can rerun a self-contained `tv -p '...'` line (the format
-- `printScriptCmd` emits). The `-f` flag is used to inject prql_ at
-- runtime in @appMain@; only `-p` and `-s` are extracted here.
parseArgs :: [Text] -> CliArgs
parseArgs args0 =
  let noSign_    = elem "+n" args0
      args1      = filter (/= "+n") args0
      (session_, args2) = extractFlag "-s" args1
      (prql_,    args3) = extractFlag "-p" args2
      (script_,  args4) = extractFlag "-f" args3
      toK s      = Key.tokenizeKeys s
      base       = defArgs & #noSign .~ noSign_ & #session .~ session_
                           & #prql .~ prql_ & #script .~ script_
      cli = case args4 of
        ("-c" : k : _)        -> base & #keys .~ toK k & #test .~ True
        (p : "-c" : k : _)    -> base & #path .~ Just p & #keys .~ toK k & #test .~ True
        (p : _)               -> base & #path .~ Just p
        []                    -> base
  in case (cli ^. #path, prql_ >>= pathFromPrql) of
       (Nothing, Just p) -> cli & #path .~ Just p
       _                 -> cli

-- | Extract `<path>` from a PRQL string that begins with `from \`<path>\``.
-- Returns Nothing if the prefix isn't there.
pathFromPrql :: Text -> Maybe Text
pathFromPrql t = do
  rest <- T.stripPrefix "from `" (T.stripStart t)
  let (p, after) = T.breakOn "`" rest
  if T.null after then Nothing else Just p

-- | Init/shutdown socket + terminal around a mainLoop call
withTui :: Bool -> IO a -> IO a
withTui test_ f = do
  r <- Socket.bracket test_ f
  unless test_ Term.shutdown
  pure r

-- | Build the initial AppState with Lean's `.default` equivalents for
-- non-constructor fields. Kept here so Main matches the Lean record literal
-- `{ stk, vs := .default, theme := th, info := {} }`.
initState :: ViewStack AdbcTable -> Theme.State -> Bool -> Bool -> AppState
initState stk_ th tm ns =
  let (cc, m) = Common.initHandlers
  in AppState
  { stk         = stk_
  , vs          = Render.defVS
  , theme       = th
  , info        = False
  , prevScroll  = 0
  , heatMode    = 0
  , sparklines  = V.empty
  , statusCache = ColCache "" "" ""
  , aggCache    = StatusAgg.cacheEmpty
  , cmdCache    = cc
  , handlers    = m
  , testMode    = tm
  , noSign      = ns
  }

-- run app with view
runApp :: View AdbcTable -> Bool -> Bool -> Bool -> Theme.State -> Vector Text -> Maybe Text -> IO AppState
runApp v0 pipe test_ ns th ks initialPrql = do
  when (pipe && not test_) $ do
    ok <- Term.reopenTty
    unless ok $ do
      TIO.hPutStrLn stderr
        "Error: stdin was piped but /dev/tty is not accessible. \
        \Run `tv` in an interactive terminal, or pass a file path."
      exitWith (ExitFailure 1)
  v <- case initialPrql of
    Just q | not (T.null q) -> do
      mv' <- applyInitialPrql q v0
      case mv' of
        Just v' -> pure v'
        Nothing -> do
          TIO.hPutStrLn stderr ("Error: -p PRQL failed to apply: " <> q)
          exitWith (ExitFailure 1)
    _ -> pure v0
  _ <- Term.init
  let stk_ = ViewStack { hd = v, tl = [] }
  finalSt <- withTui test_ (Common.mainLoop (initState stk_ th test_ ns) test_ ks)
  printScriptCmd initialPrql finalSt
  pure finalSt

-- | Re-execute the table with the user's PRQL. If the PRQL begins with
-- `from \`...\`` it's used as the entire base (so the recreate line tv
-- prints — which always carries a from-clause — round-trips). Otherwise
-- it's appended to the existing `from <path>` base.
applyInitialPrql :: Text -> View AdbcTable -> IO (Maybe (View AdbcTable))
applyInitialPrql prqlOps v = do
  let oldQ = v ^. #nav % #tbl % #query
      newBase = case pathFromPrql prqlOps of
        Just _  -> prqlOps
        Nothing -> oldQ ^. #base <> " | " <> prqlOps
      newQ = oldQ { Prql.base = newBase }
  total <- AdbcTable.queryCount newQ
  mTbl <- AdbcTable.requery newQ total
  case mTbl of
    Nothing   -> pure Nothing
    Just tbl' -> pure (View.rebuild v tbl' 0 V.empty 0)

-- | After the session, print a single self-contained line on stderr
-- that recreates the top view:
--
--   @tv -p 'from `<path>` | <ops>'@
--
-- The from-clause makes the PRQL valid on its own (paste into prqlc /
-- psql / etc), and the corresponding parseArgs branch lets `tv -p`
-- consume that exact string back without a positional path argument.
--
-- Only VkTbl views are scriptable (their query.base is `from <path>`).
-- Derived views (Freq, Meta, …) live on top of a temp table whose name
-- isn't portable, so we walk the stack to the deepest VkTbl view and
-- print that one — typically the original loaded table with its
-- accumulated filter/sort/derive ops. This makes Q (which quits without
-- popping) emit something meaningful even when the user is sitting on
-- a Freq or Meta view.
printScriptCmd :: Maybe Text -> AppState -> IO ()
printScriptCmd initialPrql a =
  let s = a ^. #stk
      stackList = (s ^. #hd) : (s ^. #tl)
      mTbl = listToMaybe [v | v <- stackList, v ^. #vkind == VkTbl, not (T.null (v ^. #path))]
  in case mTbl of
       Just v -> do
         let p     = v ^. #path
             ops   = Prql.renderOps (v ^. #nav % #tbl % #query)
             pre   = case initialPrql of
                       Just q | Just _ <- pathFromPrql q
                                -- already had its own from-clause, strip
                                -- it so we don't double up; only keep the
                                -- ops portion.
                                -> fromMaybe q $ T.stripPrefix
                                                   ("from `" <> p <> "` | ")
                                                   q
                       Just q  -> q
                       Nothing -> ""
             prql_ = case (T.null pre, T.null ops) of
                       (True,  True)  -> ""
                       (False, True)  -> pre
                       (True,  False) -> ops
                       (False, False) -> pre <> " | " <> ops
             fullPrql = "from `" <> p <> "`"
                     <> if T.null prql_ then "" else " | " <> prql_
         -- Stderr so the hint stays out of any pipe consuming tv's
         -- table dump on stdout. Terminals merge stderr to the screen,
         -- so the user still sees the line. No `# ` comment prefix —
         -- this line is meant to be copy-pasted as the next command.
         TIO.hPutStrLn stderr ("tv -p " <> shellQuote fullPrql)
       Nothing -> pure ()

-- | Quote a value for the shell. Three tiers, picked to keep PRQL
-- pipelines readable when copy-pasted:
--   * all-safe chars → no quotes;
--   * no $/`/\\/" → double quotes (so PRQL's `name == 'foo'` reads
--     plainly without `'\\''` noise);
--   * otherwise → single quotes with the standard `'\\''` escape, so
--     backticked column names like `sort {this.\`name\`}` stay literal.
shellQuote :: Text -> Text
shellQuote s
  | T.all safe   s = s
  | T.all dqSafe s = "\"" <> s <> "\""
  | otherwise      = "'" <> T.replace "'" "'\\''" s <> "'"
  where
    safe c   = isAlphaNum c || c `elem` ("/._-:" :: String)
    dqSafe c = c `notElem` ("\"$`\\" :: String)
    isAlphaNum c = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')

-- run from TSV string result
runTsv
  :: Either Text Text -> Text -> Bool -> Bool -> Bool -> Theme.State -> Vector Text -> Maybe Text
  -> IO (Maybe AppState)
runTsv r nm pipe test_ ns th ks initialPrql = case r of
  Left e -> do
    TIO.hPutStrLn stderr ("Parse error: " <> e)
    pure Nothing
  Right tsv -> do
    madbc <- AdbcTable.fromTsv tsv
    case madbc of
      Nothing   -> do TIO.hPutStrLn stderr "Empty table"; pure Nothing
      Just adbc -> case View.fromTbl adbc nm 0 V.empty 0 of
        Nothing -> do TIO.hPutStrLn stderr "Empty table"; pure Nothing
        Just v  -> Just <$> runApp v pipe test_ ns th ks initialPrql

-- output table as plain text
outputTable :: AppState -> IO ()
outputTable a = do
  txt <- Ops.toText (View.tbl (a ^. #stk))
  TIO.putStrLn txt

-- | Init backend + log file + tmp dir. Returns backend error ("" on success).
initLogging :: IO Text
initLogging = do
  logPath <- Log.path
  Log.setLog (T.pack logPath)
  Tmp.tmpDir `seq` pure ()
  tdStr <- Log.dir
  Log.write "init" ("tmpdir=" <> T.pack tdStr)
  initRes <- try AdbcTable.init :: IO (Either SomeException Text)
  case initRes of
    Left e    -> pure ("Backend init error: " <> T.pack (show e))
    Right err -> pure (if T.null err then "" else "Backend init failed: " <> err)

-- | Always tear down backend + temp dir, even on exception.
finallyCleanup :: IO () -> IO ()
finallyCleanup act = do
  r <- try act :: IO (Either SomeException ())
  AdbcTable.shutdown
  Tmp.cleanupTmp
  case r of
    Right _ -> pure ()
    Left e  -> TIO.hPutStrLn stderr ("Error: " <> T.pack (show e))

-- | Session restore (`-s name`): load saved stack, run main loop.
runSession :: Text -> Theme.State -> Bool -> Bool -> Vector Text -> IO ()
runSession sessName theme testMode noSign_ keys_ = do
  msess <- Session.load noSign_ sessName
  case msess of
    Just stk_ -> do
      _ <- Term.init
      _ <- withTui testMode
             (Common.mainLoop (initState stk_ theme testMode noSign_) testMode keys_)
      pure ()
    Nothing -> TIO.hPutStrLn stderr ("Session not found: " <> sessName)

-- | Dispatch folder / source / file by path shape.
dispatchPath :: Text -> Vector Text -> Bool -> Bool -> Bool -> Theme.State -> Maybe Text -> IO ()
dispatchPath path_ keys_ testMode noSign_ pipeMode theme initialPrql = do
  let srcCfg = Source.findSource path_
      go v = runApp v pipeMode testMode noSign_ theme keys_ initialPrql
  isDir <- doesDirectoryExist (T.unpack path_)
  if T.null path_ || isJust srcCfg || isDir then do
    let p = if T.null path_ then "." else path_
    -- Source-driven direct entry (e.g. tv osquery://groups): try `open` for
    -- paths that name a single thing (no trailing '/') under a source prefix.
    handled <- case srcCfg of
      Just src | not (T.null (src ^. #pfx))
               , not (T.isSuffixOf "/" p)
               , T.length p > T.length (src ^. #pfx) -> do
          r <- Source.runOpen noSign_ src p
          case r of
            Source.OpenAsTable adbc ->
              case View.fromTbl adbc p 0 V.empty 0 of
                Just v  -> do _ <- go v; pure True
                Nothing -> do TIO.hPutStrLn stderr ("Empty: " <> p); pure True
            _ -> pure False  -- fall through to folder listing
      _ -> pure False
    unless handled $ do
      mv <- Folder.mkView noSign_ p 1
      case mv of
        Just v  -> do _ <- go v; pure ()
        Nothing -> TIO.hPutStrLn stderr ("Cannot browse: " <> p)
  else if T.isSuffixOf ".txt" path_ then do
    content <- TIO.readFile (T.unpack path_)
    _ <- runTsv (TextParse.fromText content) path_ pipeMode testMode noSign_ theme keys_ initialPrql
    pure ()
  else if T.isSuffixOf ".gz" path_ && not (FileFormat.isData path_) then do
    -- Smart: try read_csv for unrecognized .gz (handles decompression natively)
    mv <- FileFormat.readCsv path_
    case mv of
      Just v  -> do _ <- go v; pure ()
      Nothing -> FileFormat.viewFile testMode path_
  else do
    r <- try (FileFormat.openFile path_) :: IO (Either SomeException (Maybe (View AdbcTable)))
    mv <- case r of
      Right m -> pure m
      Left e  -> do Log.write "open" (T.pack (show e)); pure Nothing
    case mv of
      Just v  -> do _ <- go v; pure ()
      Nothing -> FileFormat.viewFile testMode path_

-- ----------------------------------------------------------------------------
-- Non-interactive (agent/script) flags: --prql-funcs, --schema, --emit, --doc
--
-- All of these write to stdout and exit; none of them touch Term.init or
-- paint a TUI. Errors go to stderr. DuckDB is initialized on demand for
-- flags that need it (schema, emit); --prql-funcs and --doc don't.
-- ----------------------------------------------------------------------------

-- | Dump embedded funcs.prql to stdout.
dumpFuncs :: IO ()
dumpFuncs = BS.hPut stdout funcsBytes

-- | Initialize DuckDB for non-interactive flags; abort with stderr + exit
-- code 1 if backend init fails. Returns nothing (just side-effect).
initBackend :: IO ()
initBackend = do
  err <- AdbcTable.init
  unless (T.null err) $ do
    TIO.hPutStrLn stderr ("Backend init failed: " <> err)
    exitWith (ExitFailure 1)

-- | Check that a path refers to a local file. URL schemes and
-- directories are rejected with an actionable error. We scope --schema
-- and --emit to local files in this first cut — remote sources need
-- auth/config plumbing that's not worth replicating for one-shot CLI.
requireLocalFile :: Text -> IO ()
requireLocalFile p
  | T.null p = die "path is empty"
  | T.isInfixOf "://" p = die ("remote sources not supported; got " <> p)
  | otherwise = do
      isDir <- doesDirectoryExist (T.unpack p)
      when isDir $ die ("path is a directory, not a file: " <> p)
      ok <- doesFileExist (T.unpack p)
      unless ok $ die ("no such file: " <> p)
  where
    die msg = do TIO.hPutStrLn stderr ("Error: " <> msg); exitWith (ExitFailure 1)

-- | Print column schema for a local data file as TSV: header
-- @column\ttype\tnullable@, one row per column.
dumpSchema :: Text -> IO ()
dumpSchema path_ = do
  requireLocalFile path_
  initBackend
  let sql = "DESCRIBE SELECT * FROM '" <> escSql path_ <> "'"
  r <- try (Conn.query sql) :: IO (Either SomeException Conn.QueryResult)
  case r of
    Left e -> do
      TIO.hPutStrLn stderr ("DESCRIBE failed for " <> path_ <> ": " <> T.pack (show e))
      AdbcTable.shutdown
      exitWith (ExitFailure 1)
    Right qr -> do
      TIO.putStrLn "column\ttype\tnullable"
      -- DuckDB DESCRIBE columns: column_name, column_type, null, key, default, extra
      let nc = Conn.ncols qr
          colIdx name = V.findIndex (== name) (qr ^. #colNames)
          nameIx = fromMaybe 0 (colIdx "column_name")
          typeIx = fromMaybe 1 (colIdx "column_type")
          nullIx = fromMaybe 2 (colIdx "null")
      when (nc >= 3) $
        forM_ [0 .. Conn.nrows qr - 1] $ \i -> do
          let nm  = Conn.cellStr qr i nameIx
              ty  = Conn.cellStr qr i typeIx
              nul = Conn.cellStr qr i nullIx
          TIO.putStrLn (nm <> "\t" <> ty <> "\t" <> nul)
      AdbcTable.shutdown

-- | DuckDB COPY option clause for an --emit format keyword.
emitOpt :: Text -> Maybe Text
emitOpt "csv"    = Just "(FORMAT CSV, HEADER true)"
emitOpt "tsv"    = Just "(FORMAT CSV, HEADER true, DELIMITER '\t')"
emitOpt "json"   = Just "(FORMAT JSON, ARRAY true)"
emitOpt "ndjson" = Just "(FORMAT JSON)"
emitOpt _        = Nothing

-- | Execute a PRQL pipeline (or raw path dump) and write results to
-- stdout in the requested format, then exit. Accepts the same subset of
-- flags as the main CLI: positional path, -p PRQL, -f FILE.
runEmit :: Text -> [Text] -> IO ()
runEmit fmt rest = case emitOpt fmt of
  Nothing -> do
    TIO.hPutStrLn stderr
      ("Error: --emit FMT must be one of csv, tsv, json, ndjson; got " <> fmt)
    exitWith (ExitFailure 2)
  Just opt -> do
    let cli0 = parseArgs rest
    prqlText <- case (cli0 ^. #prql, cli0 ^. #script) of
      (Just q, _)        -> pure (Just q)
      (Nothing, Just f)  -> (Just . T.stripEnd) <$> TIO.readFile (T.unpack f)
      (Nothing, Nothing) -> pure Nothing
    let path_ = fromMaybe "" (cli0 ^. #path)
    when (T.null path_ && isNothing prqlText) $ do
      TIO.hPutStrLn stderr
        "Error: --emit needs at least a path or -p PRQL (or -f FILE)"
      exitWith (ExitFailure 2)
    -- Local-only: PRQL without a from-clause still needs the file resolved.
    unless (T.null path_) $ requireLocalFile path_
    initBackend
    -- Build the PRQL. If user gave PRQL with its own `from`-clause, use
    -- it as-is; if they gave `-p ops` with a path, build `from <path> | ops`;
    -- if no PRQL, raw SELECT * from the file.
    innerSql <- case prqlText of
      Nothing ->
        pure (Right ("SELECT * FROM '" <> escSql path_ <> "'"))
      Just q -> do
        let q' = case pathFromPrql q of
                   Just _  -> q
                   Nothing -> "from `" <> path_ <> "` | " <> q
        m <- Prql.compile q'
        pure $ case m of
          Just sql -> Right (stripSemi sql)
          Nothing  -> Left q'
    case innerSql of
      Left q' -> do
        TIO.hPutStrLn stderr ("Error: PRQL compile failed: " <> q')
        AdbcTable.shutdown
        exitWith (ExitFailure 1)
      Right sql -> do
        hSetBuffering stdout NoBuffering
        let copySql = "COPY (" <> sql <> ") TO '/dev/stdout' " <> opt
        r <- try (Conn.query copySql) :: IO (Either SomeException Conn.QueryResult)
        AdbcTable.shutdown
        case r of
          Left e -> do
            TIO.hPutStrLn stderr ("Error: emit failed: " <> T.pack (show e))
            exitWith (ExitFailure 1)
          Right _ -> pure ()

-- | Dump the command menu as TSV: handler, key, viewCtx, hint, label.
-- Entries with empty label are skipped (those are either internal-only
-- dispatch targets or key-only bindings without a menu entry).
dumpCommands :: IO ()
dumpCommands = do
  TIO.putStrLn "handler\tkey\tviewCtx\thint\tlabel"
  let entries = V.map fst Common.commands
  V.forM_ entries $ \e ->
    unless (T.null (e ^. #label)) $
      TIO.putStrLn $ T.intercalate "\t"
        [ toString (e ^. #cmd)
        , e ^. #key
        , e ^. #viewCtx
        , if e ^. #hint then "1" else "0"
        , e ^. #label
        ]

helpText :: Text
helpText = T.unlines
  [ "tv — terminal viewer for tabular data"
  , ""
  , "Usage: tv [options] [path]"
  , ""
  , "Examples:"
  , "  tv data.csv                  CSV, Parquet, JSON/NDJSON/JSONL, SQLite,"
  , "  tv sample.parquet            DuckDB, XLSX, Avro, Arrow/Feather"
  , "  tv archive.csv.gz            (gzipped variants auto-detected)"
  , "  tv folder/                   Folder: browse files like a mini shell"
  , "  ls | tv                      Read TSV/CSV from stdin"
  , ""
  , "  tv s3://overturemaps-us-west-2/release/ +n   S3 (+n = no-sign/anon)"
  , "  tv ftp://ftp.nyse.com/                       FTP listing"
  , "  tv hf://                                      List HuggingFace datasets"
  , "  tv hf://datasets/openai/gsm8k                 HF dataset root"
  , "  tv osquery://                                 List osquery tables"
  , "  tv osquery://processes                        Run a specific table"
  , "  tv pg://user@host/dbname                      PostgreSQL (table list)"
  , "  tv rest://example.com/api/users               REST JSON endpoint"
  , ""
  , "Options:"
  , "  -c KEYS             Test mode: replay KEYS, render once, exit."
  , "  -s NAME             Restore session NAME (~/.cache/tv/sessions/NAME.json)."
  , "  -p PRQL             Apply PRQL pipeline to the input as initial ops"
  , "                      (e.g. -p 'filter score > 80 | sort name')."
  , "                      If PRQL begins with `from \\`<path>\\``, the"
  , "                      positional path argument is optional."
  , "  -f FILE             Read PRQL from FILE (multi-line, may contain"
  , "                      `let` definitions). Shorthand for `-p \"$(<FILE)\"`."
  , "  +n                  no-sign mode for S3 and similar anonymous sources."
  , "  -h, --help          Show this help and exit."
  , ""
  , "Non-interactive (agent/script) flags:"
  , "  --prql-funcs        Dump the embedded funcs.prql prelude to stdout."
  , "  --schema PATH       Print column schema (name\\ttype\\tnullable) for a"
  , "                      local data file; no TUI. Remote URLs not supported."
  , "  --emit FMT [args]   Run PRQL / raw path and stream results to stdout."
  , "                      FMT = csv | tsv | json | ndjson. Accepts -p PRQL,"
  , "                      -f FILE, or a positional path. Local files only."
  , "  --doc commands      Dump the command menu as TSV"
  , "                      (handler\\tkey\\tviewCtx\\thint\\tlabel)."
  , ""
  , "On exit, tv prints a self-contained `tv -p '...'` line on stderr"
  , "(the PRQL carries its own from-clause), so you can copy-paste it"
  , "to rerun the same view, or strip `tv -p '…'` to use the PRQL in"
  , "another tool (prqlc, psql, …)."
  , ""
  , "In-app: press `I` for the quick reference, or Space for the command menu."
  ]

-- main entry point: init backend, parse args, run app
appMain :: [Text] -> IO ()
appMain args
  | any (`elem` (["-h", "--help"] :: [Text])) args = TIO.putStr helpText
  | otherwise = case args of
      -- Non-interactive flags first: they exit without ever touching Term.init.
      -- Keep them strictly positional so help text remains self-describing.
      ["--prql-funcs"]            -> dumpFuncs >> exitSuccess
      ["--schema", p]             -> dumpSchema p >> exitSuccess
      ("--emit" : fmt : rest)     -> runEmit fmt rest >> exitSuccess
      ["--doc", "commands"]       -> dumpCommands >> exitSuccess
      _                           -> appMainInteractive args

-- | The original TUI-bound entry path, split out so the new non-interactive
-- flags can short-circuit without threading through it.
appMainInteractive :: [Text] -> IO ()
appMainInteractive args = do
  let cli0 = parseArgs args
  -- -f FILE reads a PRQL script (multi-line, with `let`s) and feeds it
  -- as if -p had been passed verbatim. -p still wins if both given.
  prql_  <- case (cli0 ^. #prql, cli0 ^. #script) of
    (Just q, _)       -> pure (Just q)
    -- Strip trailing whitespace; PRQL's parser is intolerant of a
    -- terminating newline / blank line at EOF.
    (Nothing, Just f) -> (Just . T.stripEnd) <$> TIO.readFile (T.unpack f)
    (Nothing, Nothing) -> pure Nothing
  let cli = cli0 & #prql .~ prql_
        -- re-extract path from the file's from-clause if -f provided one
        & (\c -> case (c ^. #path, prql_ >>= pathFromPrql) of
                   (Nothing, Just p) -> c & #path .~ Just p
                   _                 -> c)
      CliArgs { path = path_, keys = keys_, noSign = noSign_ } = cli
  envTest  <- isJust <$> lookupEnv "TV_TEST_MODE"
  let testMode = cli ^. #test || envTest
  pipeMode <- if testMode then pure False else not <$> Term.isattyStdin
  theme    <- Theme.stateInit
  err      <- initLogging
  unless (T.null err) $ TIO.hPutStrLn stderr err
  when (T.null err) $ case cli ^. #session of
    Just sessName -> finallyCleanup (runSession sessName theme testMode noSign_ keys_)
    Nothing
      | pipeMode && isNothing path_ -> do
          stdinRes <- TextParse.fromStdin
          m <- runTsv stdinRes "stdin" True testMode noSign_ theme keys_ prql_
          maybe (pure ()) outputTable m
      | otherwise ->
          finallyCleanup
            (dispatchPath (fromMaybe "" path_) keys_ testMode noSign_ pipeMode theme prql_)


main :: IO ()
main = do
  args <- getArgs
  r <- try (appMain (map T.pack args)) :: IO (Either SomeException ())
  case r of
    Right _ -> pure ()
    -- Let ExitCode propagate cleanly (exitWith); only format other errors.
    Left e  -> maybe (TIO.hPutStrLn stderr ("Error: " <> T.pack (show e))) exitWith
                     (fromException e :: Maybe ExitCode)
