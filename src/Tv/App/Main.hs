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
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (doesDirectoryExist)
import System.Environment (getArgs, lookupEnv)
import System.Exit (ExitCode(..), exitWith)
import System.IO (stderr)

import qualified Tv.App.Common as Common
import Tv.App.Types (AppState(..))
import Tv.Types (ColCache(..), ViewKind(..))
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Prql as Prql
import qualified Tv.Data.DuckDB.Table as AdbcTable
import Tv.Data.DuckDB.Table (AdbcTable)
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
  }
makeFieldLabelsNoPrefix ''CliArgs

defArgs :: CliArgs
defArgs = CliArgs { path = Nothing, keys = V.empty, test = False, noSign = False, session = Nothing, prql = Nothing }

-- extract flag with value, return (value?, remaining args)
extractFlag :: Text -> [Text] -> (Maybe Text, [Text])
extractFlag flag (f : v : rest)
  | f == flag = (Just v, rest)
  | otherwise = let (r, rest') = extractFlag flag (v : rest) in (r, f : rest')
extractFlag _ other = (Nothing, other)

-- parse args: path?, -c keys?, test mode, +n, -s session, -p PRQL
parseArgs :: [Text] -> CliArgs
parseArgs args0 =
  let noSign_    = elem "+n" args0
      args1      = filter (/= "+n") args0
      (session_, args2) = extractFlag "-s" args1
      (prql_,    args3) = extractFlag "-p" args2
      toK s      = Key.tokenizeKeys s
      base       = defArgs & #noSign .~ noSign_ & #session .~ session_ & #prql .~ prql_
  in case args3 of
       ("-c" : k : _) ->
         base & #keys .~ toK k & #test .~ True
       (p : "-c" : k : _) ->
         base & #path .~ Just p & #keys .~ toK k & #test .~ True
       (p : _) ->
         base & #path .~ Just p
       [] ->
         base

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

-- | Re-execute the initial table with `oldBase | prql`, return rebuilt view.
applyInitialPrql :: Text -> View AdbcTable -> IO (Maybe (View AdbcTable))
applyInitialPrql prqlOps v = do
  let oldQ = v ^. #nav % #tbl % #query
      newQ = oldQ { Prql.base = oldQ ^. #base <> " | " <> prqlOps }
  total <- AdbcTable.queryCount newQ
  mTbl <- AdbcTable.requery newQ total
  case mTbl of
    Nothing   -> pure Nothing
    Just tbl' -> pure (View.rebuild v tbl' 0 V.empty 0)

-- | After the session, print a CLI command that recreates a view from
-- the stack. Format: `tv <path> [-p '<prql>']`. The PRQL is the initial
-- -p (if any) concatenated with the ops added interactively.
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
             pre   = fromMaybe "" initialPrql
             prql_ = case (T.null pre, T.null ops) of
                       (True,  True)  -> ""
                       (False, True)  -> pre
                       (True,  False) -> ops
                       (False, False) -> pre <> " | " <> ops
             cmd = "tv " <> shellQuote p
                <> if T.null prql_ then "" else " -p " <> shellQuote prql_
         -- Stderr so the recreate hint stays out of any pipe consuming
         -- tv's table dump on stdout. Terminals merge stderr to the
         -- screen, so users still see the line.
         TIO.hPutStrLn stderr ("# recreate: " <> cmd)
       Nothing -> pure ()

-- | Single-quote a value for the shell, escaping embedded quotes.
shellQuote :: Text -> Text
shellQuote s
  | T.all safe s = s
  | otherwise    = "'" <> T.replace "'" "'\\''" s <> "'"
  where safe c = isAlphaNum c || c `elem` ("/._-:" :: String)
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
  , "  +n                  no-sign mode for S3 and similar anonymous sources."
  , "  -h, --help          Show this help and exit."
  , ""
  , "On exit, tv prints a recreate command (path + -p PRQL) so you can"
  , "rerun the same view from the shell."
  , ""
  , "In-app: press `I` for the quick reference, or Space for the command menu."
  ]

-- main entry point: init backend, parse args, run app
appMain :: [Text] -> IO ()
appMain args
  | any (`elem` (["-h", "--help"] :: [Text])) args = TIO.putStr helpText
  | otherwise = do
  let cli = parseArgs args
      CliArgs { path = path_, keys = keys_, noSign = noSign_ } = cli
      prql_ = cli ^. #prql
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
