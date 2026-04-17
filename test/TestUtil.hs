{-# LANGUAGE ScopedTypeVariables #-}
module TestUtil
  ( tvHaskBin
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

import Control.Exception (catch, SomeException)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Char (isAlpha, isDigit)
import Data.Text (Text)
import Tv.Types (getD)
import qualified Data.Text as T
import System.IO (withFile, IOMode (..))
import System.Process (readProcessWithExitCode, readCreateProcessWithExitCode, proc, CreateProcess(..))
import System.Environment (getEnvironment)
import System.Exit (ExitCode (..))
import System.IO.Unsafe (unsafePerformIO)

tvHaskBin :: FilePath
tvHaskBin = unsafePerformIO resolveBin
{-# NOINLINE tvHaskBin #-}

resolveBin :: IO FilePath
resolveBin = do
  (code, out, _) <- readProcessWithExitCode "cabal" ["-v0", "list-bin", "tv"] ""
  case code of
    ExitSuccess -> let p = filter (/= '\n') out
                   in if null p then pure fallback else pure p
    _           -> pure fallback
  where fallback = "dist-newstyle/build/x86_64-linux/ghc-9.6.6/tv-hask-0.1.0.0/x/tv/build/tv/tv"

runHask :: Text -> FilePath -> [String] -> IO Text
runHask keys file extraArgs = do
  let args = (if null file then [] else [file]) ++ extraArgs ++ ["-c", T.unpack keys]
  (_, out, _) <- readProcessWithExitCode tvHaskBin args ""
  pure (T.pack out)

-- | Drive `tv` through a real pty via test/pty_run.py. `keys` is passed
-- verbatim to pty_run.py, which expands only \\r/\\n/\\t/\\b/\\e — so send
-- a CSI Up as "\\e[A", not "\\x1B[A".
runPty :: String -> FilePath -> IO Text
runPty keys file = do
  -- pty_run.py's TV env var: point it at the binary in *this* build tree,
  -- not the hard-coded main-repo fallback (matters in worktrees).
  parentEnv <- getEnvironment
  let envOverride = ("TV", tvHaskBin) : filter ((/= "TV") . fst) parentEnv
      cp = (proc "test/pty_run.py" [file, keys]) { env = Just envOverride }
  (_, out, _) <- readCreateProcessWithExitCode cp ""
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

