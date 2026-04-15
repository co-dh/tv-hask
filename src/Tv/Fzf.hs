{-
  Fzf: helpers for running fzf picker
  Suspends terminal, spawns fzf, returns selection
  In testMode, returns first value without spawning fzf

  Literal port of Tc/Tc/Fzf.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Fzf
  ( setTestMode
  , getTestMode
  , fzfCore
  , fzf
  , fzfIdx
  , parseFlatSel
  , cmdMode
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (unless, when)
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Environment (lookupEnv)
import System.IO (hClose, hFlush)
import System.IO.Unsafe (unsafePerformIO)
import System.Process
  ( CreateProcess(..), StdStream(..), proc
  , createProcess, waitForProcess
  )
import Text.Read (readMaybe)

import qualified Tv.CmdConfig as CmdConfig
import Tv.Types (ViewKind, viewKindCtxStr)
import qualified Tv.Term as Term

-- | Global testMode flag (set by App.main)
testMode :: IORef Bool
testMode = unsafePerformIO (newIORef False)
{-# NOINLINE testMode #-}

-- | Set testMode
setTestMode :: Bool -> IO ()
setTestMode = writeIORef testMode

-- | Get testMode
getTestMode :: IO Bool
getTestMode = readIORef testMode

-- | Core fzf: testMode returns first line, else spawn fzf
-- Uses --tmux popup if in tmux (keeps table visible), otherwise compact at bottom
-- poll: optional callback invoked in loop while fzf runs (tmux only, for live socket dispatch)
fzfCore :: Vector Text -> Text -> IO () -> IO Text
fzfCore opts input poll = do
  tm <- getTestMode
  if tm
    then pure (headD "" (filter (not . T.null) (T.splitOn "\n" input)))
    else do
      inTmux <- fmap (maybe False (const True)) (lookupEnv "TMUX")
      let lines_ = filter (not . T.null) (T.splitOn "\n" input)
          popupH = min (length lines_ + 2) 15  -- fit content, cap at 15
          -- measure visible width: strip hidden prefix when --with-nth hides leading fields
          visLines = if V.any (T.isPrefixOf "--with-nth=2") opts
            then map (\l -> case T.splitOn "\t" l of
                              _ : rest -> T.intercalate "\t" rest
                              _        -> l) lines_
            else lines_
          maxW = foldl (\m l -> max m (T.length l)) 0 visLines
          popupW = min (max (maxW + 4) 50) 80  -- fit content, floor 50 for typing
          baseArgs = if inTmux
            then V.fromList
                   [ T.pack ("--tmux=bottom," ++ show popupW ++ "," ++ show popupH)
                   , "--layout=reverse", "--exact", "+i" ]
            else V.fromList
                   [ T.pack ("--height=" ++ show (popupH + 1))
                   , "--layout=reverse", "--exact", "+i" ]
      unless inTmux Term.shutdown
      let argList = map T.unpack (V.toList (baseArgs V.++ opts))
      (Just hin, Just hout, _, ph) <- createProcess
        (proc "fzf" argList) { std_in = CreatePipe, std_out = CreatePipe }
      TIO.hPutStr hin input
      hFlush hin
      hClose hin
      -- Read stdout in background; poll socket while fzf popup is open
      done <- newIORef False
      outRef <- newIORef (T.empty :: Text)
      _ <- forkIO $ do
        out <- TIO.hGetContents hout
        writeIORef outRef out
        writeIORef done True
      let loop = do
            d <- readIORef done
            unless d $ do
              poll
              threadDelay 30000  -- 30 ms
              loop
      loop
      out <- readIORef outRef
      _ <- waitForProcess ph
      -- Clear after re-init: fzf inline renders below termbox area, leaving residue
      when (not inTmux) $ do
        _ <- Term.init
        Term.clear
        Term.present
      pure (T.strip out)
  where
    headD d []    = d
    headD _ (x:_) = x

-- | Single select: returns none if empty/cancelled
fzf :: Vector Text -> Text -> IO (Maybe Text)
fzf opts input = do
  out <- fzfCore opts input (pure ())
  pure (if T.null out then Nothing else Just out)

-- | Index select: testMode returns 0
fzfIdx :: Vector Text -> Vector Text -> IO (Maybe Int)
fzfIdx opts items = do
  tm <- getTestMode
  if tm
    then pure (if V.null items then Nothing else Just 0)
    else do
      let numbered = V.imap (\i s -> T.pack (show i) <> "\t" <> s) items
          input = T.intercalate "\n" (V.toList numbered)
      out <- fzfCore (V.fromList ["--with-nth=2.."] V.++ opts) input (pure ())
      if T.null out
        then pure Nothing
        else case T.splitOn "\t" out of
          []    -> pure Nothing
          (h:_) -> pure (readMaybe (T.unpack h))

-- | Build aligned menu items: "handler | ctx | key | label" with padding
flatItems :: ViewKind -> IO (Vector Text)
flatItems vk = do
  items <- CmdConfig.menuItems (viewKindCtxStr vk)
  let (maxH, maxX, maxK) = V.foldl
        (\(mh, mx, mk) (h, x, k, _) ->
           (max mh (T.length h), max mx (T.length x), max mk (T.length k)))
        (0, 0, 0) items
  pure $ V.map (\(handler, ctx, key, label) ->
    let hp = handler <> T.replicate (maxH - T.length handler) " "
        xp = ctx     <> T.replicate (maxX - T.length ctx) " "
        kp = key     <> T.replicate (maxK - T.length key) " "
    in hp <> " | " <> xp <> " | " <> kp <> " | " <> label) items

-- | Parse flat selection: extract handler name before first |
parseFlatSel :: Text -> Maybe Text
parseFlatSel sel =
  let h = T.stripEnd (headD "" (T.splitOn " | " sel))
  in if T.null h then Nothing else Just h
  where
    headD d []    = d
    headD _ (x:_) = x

-- | Command mode: space -> flat fzf menu -> return handler name
-- poll: callback invoked while fzf popup is open (for external socket dispatch + re-render)
cmdMode :: ViewKind -> IO () -> IO (Maybe Text)
cmdMode vk poll = do
  items <- flatItems vk
  if V.null items
    then pure Nothing
    else do
      let input = T.intercalate "\n" (V.toList items)
          opts  = V.fromList ["--prompt=cmd "]
      out <- fzfCore opts input poll
      if T.null out
        then pure Nothing
        else pure (parseFlatSel out)
