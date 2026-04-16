{-# LANGUAGE ScopedTypeVariables #-}
-- |
--   Shared test utilities used by MainSpec, ScreenSpec, RenderSpec, PureSpec.
--   Literal port of Tc/test/TestUtil.lean.
module TestUtil
  ( bin
  , tvHaskBin
  , log
  , run
  , runHask
  , runPty
  , isContent
  , contains
  , footer
  , header
  , dataLines
  , assert
  , hasFile
  , hasCmd
  , cachedCheck
  ) where

import Prelude hiding (log)
import Control.Exception (catch, SomeException)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Char (isAlpha, isDigit)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.IO (hFlush, stderr, withFile, IOMode (..), Handle)
import qualified System.IO as IO
import System.Process (readProcessWithExitCode, readCreateProcessWithExitCode, proc, CreateProcess(..))
import System.Environment (getEnvironment)
import System.Exit (ExitCode (..))

-- | Path to the tv binary the tests shell out to. In the Lean reference this
--   was `.lake/build/bin/tv`; in the Haskell port it's the cabal-built binary.
bin :: FilePath
bin = "dist-newstyle/build/x86_64-linux/ghc-9.6.6/tv-hask-0.1.0.0/x/tv/build/tv/tv"

-- | Alias kept for call sites that explicitly spawn the Haskell binary.
tvHaskBin :: FilePath
tvHaskBin = bin

log :: Text -> IO ()
log msg = withFile "test.log" AppendMode $ \h -> do
  TIO.hPutStrLn h msg
  hFlush h

run :: Text -> FilePath -> [String] -> IO Text
run = runWith bin

-- | Same as 'run' but spawns the Haskell-built `tv` binary.
runHask :: Text -> FilePath -> [String] -> IO Text
runHask = runWith tvHaskBin

-- | Drive the `tv` binary through a real pty via test/pty_run.py, sending
-- raw byte sequences (e.g. "\x1b[A" for arrow-up). Unlike runHask, this
-- exercises the full Term.pollEvent path — cbreak-mode tty input + escape
-- sequence decoding — so it catches regressions to the interactive key
-- path that the `-c` flag's in-process tokenizer bypasses.
--
-- `keys` is passed as argv[2] to pty_run.py, which translates only the
-- \\r/\\n/\\t/\\b/\\e escapes but leaves raw bytes literal — so pass
-- "\\e[A" (not "\x1B[A") to send a CSI Up.
runPty :: String -> FilePath -> IO Text
runPty keys file = do
  log (T.pack "  runPty: " <> T.pack file <> T.pack " keys=" <> T.pack (show keys))
  -- Force pty_run.py to use the freshly-built binary in *this* tree
  -- (which may be a worktree), not the hard-coded main-repo path.
  parentEnv <- getEnvironment
  let envOverride = ("TV", tvHaskBin) : filter ((/= "TV") . fst) parentEnv
      cp = (proc "test/pty_run.py" [file, keys]) { env = Just envOverride }
  (code, out, err) <- readCreateProcessWithExitCode cp ""
  case err of
    "" -> pure ()
    _  -> log (T.pack "  stderr: " <> T.strip (T.pack err))
  case code of
    ExitSuccess   -> pure ()
    ExitFailure n -> log (T.pack "  exit: " <> T.pack (show n))
  log (T.pack "  done")
  pure (T.pack out)

runWith :: FilePath -> Text -> FilePath -> [String] -> IO Text
runWith exe keys file extraArgs = do
  log (T.pack "  run: " <> T.pack file <> T.pack " " <> T.pack (show extraArgs) <> T.pack " keys=" <> keys)
  let args = (if null file then [] else [file]) ++ extraArgs ++ ["-c", T.unpack keys]
  (code, out, err) <- readProcessWithExitCode exe args ""
  case err of
    "" -> pure ()
    _  -> log (T.pack "  stderr: " <> T.strip (T.pack err))
  case code of
    ExitSuccess   -> pure ()
    ExitFailure n -> log (T.pack "  exit: " <> T.pack (show n))
  log (T.pack "  done")
  pure (T.pack out)

isContent :: Text -> Bool
isContent l = T.any (\c -> isAlpha c || isDigit c) l

contains :: Text -> Text -> Bool
contains s sub = length (T.splitOn sub s) > 1

footer :: Text -> (Text, Text)
footer output =
  let ls = filter isContent (T.splitOn (T.pack "\n") output)
      n  = length ls
  in (getD ls (n - 2) (T.pack ""), getD ls (n - 1) (T.pack ""))

header :: Text -> Text
header output =
  let ls  = filter isContent (T.splitOn (T.pack "\n") output)
      hdr = case ls of { [] -> T.pack ""; (x:_) -> x }
  in if T.length hdr > 80 then T.drop (T.length hdr - 80) hdr else hdr

dataLines :: Text -> [Text]
dataLines output =
  let ls = filter isContent (T.splitOn (T.pack "\n") output)
      n  = length ls
  in take (n - 3) (drop 1 ls)

assert :: Bool -> String -> IO ()
assert cond msg = if cond then pure () else ioError (userError msg)

hasFile :: FilePath -> IO Bool
hasFile path =
  (withFile path ReadMode (\_ -> pure ()) >> pure True)
    `catch` (\(_ :: SomeException) -> pure False)

-- | Check if a command is available (via `command -v`)
hasCmd :: String -> IO Bool
hasCmd cmd = do
  (code, _, _) <- readProcessWithExitCode "sh" ["-c", "command -v " ++ cmd] ""
  pure (code == ExitSuccess)

-- | Generic cached bool check: run check once, cache result.
cachedCheck :: IORef (Maybe Bool) -> IO Bool -> IO Bool
cachedCheck ref check = do
  v <- readIORef ref
  case v of
    Just b  -> pure b
    Nothing -> do
      ok <- check
      writeIORef ref (Just ok)
      pure ok

-- | Lean's List.getD — indexed lookup with a default.
getD :: [a] -> Int -> a -> a
getD xs i d
  | i < 0     = d
  | otherwise = case drop i xs of { [] -> d; (x:_) -> x }
