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

import Control.Exception (SomeException, try)
import Control.Monad (unless)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Directory (doesDirectoryExist)
import System.Environment (getArgs, lookupEnv)
import System.IO (hPutStrLn, stderr)

import qualified Tv.App.Common as Common
import Tv.App.Common (AppState(..))
import qualified Tv.Data.DuckDB.Ops as Ops
import qualified Tv.Data.DuckDB.Table as AdbcTable
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.Text as TextParse
import qualified Tv.FileFormat as FileFormat
import qualified Tv.Folder as Folder
import qualified Tv.Key as Key
import qualified Tv.Render as Render
import qualified Tv.Session as Session
import qualified Tv.SourceConfig as SourceConfig
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Term as Term
import qualified Tv.Theme as Theme
import qualified Tv.UI.Info as UIInfo
import Tv.View (View, ViewStack(..))
import qualified Tv.View as View
import qualified Tv.Util as Log
import qualified Tv.Util as Socket
import Optics.Core ((&), (.~))
import Optics.TH (makeFieldLabelsNoPrefix)

-- parsed CLI arguments
data CliArgs = CliArgs
  { path    :: Maybe Text
  , keys    :: Vector Text
  , test    :: Bool
  , noSign  :: Bool
  , session :: Maybe Text   -- -s "name" session restore
  }
makeFieldLabelsNoPrefix ''CliArgs

defArgs :: CliArgs
defArgs = CliArgs { path = Nothing, keys = V.empty, test = False, noSign = False, session = Nothing }

-- extract flag with value, return (value?, remaining args)
extractFlag :: Text -> [Text] -> (Maybe Text, [Text])
extractFlag flag (f : v : rest)
  | f == flag = (Just v, rest)
  | otherwise = let (r, rest') = extractFlag flag (v : rest) in (r, f : rest')
extractFlag _ other = (Nothing, other)

-- parse args: path?, -c keys?, test mode, +n, -s session
parseArgs :: [Text] -> CliArgs
parseArgs args0 =
  let noSign_    = any (== "+n") args0
      args1      = filter (/= "+n") args0
      (session_, args2) = extractFlag "-s" args1
      toK s      = Key.tokenizeKeys s
  in case args2 of
       ("-c" : k : _) ->
         CliArgs { path = Nothing, keys = toK k, test = True, noSign = noSign_, session = session_ }
       (p : "-c" : k : _) ->
         CliArgs { path = Just p, keys = toK k, test = True, noSign = noSign_, session = session_ }
       (p : _) ->
         defArgs & #path .~ Just p & #noSign .~ noSign_ & #session .~ session_
       [] ->
         defArgs & #noSign .~ noSign_ & #session .~ session_

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
  , statusCache = ("", "", "")
  , aggCache    = StatusAgg.cacheEmpty
  , cmdCache    = cc
  , handlers    = m
  , testMode    = tm
  , noSign      = ns
  }

-- run app with view
runApp :: View AdbcTable -> Bool -> Bool -> Bool -> Theme.State -> Vector Text -> IO AppState
runApp v pipe test_ ns th ks = do
  _ <- if pipe then Term.reopenTty else pure False
  _ <- Term.init
  let stk_ = ViewStack { hd = v, tl = [] }
  withTui test_ (Common.mainLoop (initState stk_ th test_ ns) test_ ks)

-- run from TSV string result
runTsv
  :: Either Text Text -> Text -> Bool -> Bool -> Bool -> Theme.State -> Vector Text
  -> IO (Maybe AppState)
runTsv r nm pipe test_ ns th ks = case r of
  Left e -> do
    TIO.hPutStrLn stderr ("Parse error: " <> e)
    pure Nothing
  Right tsv -> do
    madbc <- AdbcTable.fromTsv tsv
    case madbc of
      Nothing   -> do TIO.hPutStrLn stderr "Empty table"; pure Nothing
      Just adbc -> case View.fromTbl adbc nm 0 V.empty 0 of
        Nothing -> do TIO.hPutStrLn stderr "Empty table"; pure Nothing
        Just v  -> Just <$> runApp v pipe test_ ns th ks

-- output table as plain text
outputTable :: AppState -> IO ()
outputTable a = do
  txt <- Ops.toText (View.tbl (Common.stk a))
  TIO.putStrLn txt

-- main entry point: init backend, parse args, run app
appMain :: [Text] -> IO ()
appMain args = do
  let cli@(CliArgs { path = path_, keys = keys_, noSign = noSign_ }) = parseArgs args
  envTest <- maybe False (const True) <$> lookupEnv "TV_TEST_MODE"
  let testMode = test cli || envTest
  pipeMode <- if testMode then pure False else not <$> Term.isattyStdin
  theme <- Theme.stateInit
  logPath <- Log.path
  Log.setLog (T.pack logPath)
  td <- Log.tmpDir `seq` pure ""
  _  <- td `seq` pure ()
  tdStr <- Log.dir
  Log.write "init" ("tmpdir=" <> T.pack tdStr)
  initRes <- try AdbcTable.init :: IO (Either SomeException Text)
  case initRes of
    Left e -> TIO.hPutStrLn stderr ("Backend init error: " <> T.pack (show e))
    Right err
      | not (T.null err) -> TIO.hPutStrLn stderr ("Backend init failed: " <> err)
      | otherwise -> runRest cli path_ keys_ testMode noSign_ pipeMode theme
  where
    runRest cli path_ keys_ testMode noSign_ pipeMode theme = do
      -- session restore: -s name
      case session cli of
        Just sessName -> do
          finallyCleanup $ do
            msess <- Session.load noSign_ sessName
            case msess of
              Just stk_ -> do
                _ <- Term.init
                _ <- withTui testMode
                       (Common.mainLoop (initState stk_ theme testMode noSign_) testMode keys_)
                pure ()
              Nothing -> TIO.hPutStrLn stderr ("Session not found: " <> sessName)
        Nothing
          | pipeMode && path_ == Nothing -> do
              stdinRes <- TextParse.fromStdin
              m <- runTsv stdinRes "stdin" True testMode noSign_ theme keys_
              case m of
                Just a  -> outputTable a
                Nothing -> pure ()
              pure ()
          | otherwise -> do
              let p0 = fromMaybe "" path_
              finallyCleanup (runPath p0 keys_ testMode noSign_ pipeMode theme)

    runPath path_ keys_ testMode noSign_ pipeMode theme = do
      srcCfg <- SourceConfig.findSource path_
      isDir  <- doesDirectoryExist (T.unpack path_)
      if T.null path_ || isJust srcCfg || isDir then do
        let p = if T.null path_ then "." else path_
        -- Config-driven direct entry (e.g. tv osquery://groups)
        handled <- case srcCfg of
          Just cfg
            | not (T.null (SourceConfig.script cfg)) && not (T.null (SourceConfig.pfx cfg)) -> do
                let rest = T.drop (T.length (SourceConfig.pfx cfg)) p
                if not (T.null rest) then do
                  m <- SourceConfig.runEnter cfg rest
                  case m of
                    Just adbc ->
                      case View.fromTbl adbc (SourceConfig.pfx cfg <> rest) 0 V.empty 0 of
                        Just v  -> do _ <- runApp v pipeMode testMode noSign_ theme keys_; pure ()
                        Nothing -> TIO.hPutStrLn stderr ("Empty: " <> p)
                    Nothing -> TIO.hPutStrLn stderr ("Cannot open: " <> p)
                  pure True
                else pure False
          _ -> pure False
        if handled then pure ()
        else do
          mv <- Folder.mkView noSign_ p 1
          case mv of
            Just v  -> do _ <- runApp v pipeMode testMode noSign_ theme keys_; pure ()
            Nothing -> TIO.hPutStrLn stderr ("Cannot browse: " <> p)
      else if T.isSuffixOf ".txt" path_ then do
        content <- TIO.readFile (T.unpack path_)
        _ <- runTsv (TextParse.fromText content) path_ pipeMode testMode noSign_ theme keys_
        pure ()
      else if T.isSuffixOf ".gz" path_ && not (FileFormat.isData path_) then do
        -- Smart: try read_csv for unrecognized .gz (handles decompression natively)
        mv <- FileFormat.readCsv path_
        case mv of
          Just v  -> do _ <- runApp v pipeMode testMode noSign_ theme keys_; pure ()
          Nothing -> FileFormat.viewFile testMode path_
      else do
        r <- try (FileFormat.openFile path_) :: IO (Either SomeException (Maybe (View AdbcTable)))
        mv <- case r of
          Right m -> pure m
          Left e  -> do Log.write "open" (T.pack (show e)); pure Nothing
        case mv of
          Just v  -> do _ <- runApp v pipeMode testMode noSign_ theme keys_; pure ()
          Nothing -> FileFormat.viewFile testMode path_

    finallyCleanup act = do
      r <- try act :: IO (Either SomeException ())
      AdbcTable.shutdown
      Log.cleanupTmp
      case r of
        Right _ -> pure ()
        Left e  -> TIO.hPutStrLn stderr ("Error: " <> T.pack (show e))


main :: IO ()
main = do
  args <- getArgs
  r <- try (appMain (map T.pack args)) :: IO (Either SomeException ())
  case r of
    Right _ -> pure ()
    Left e  -> TIO.hPutStrLn stderr ("Error: " <> T.pack (show e))
