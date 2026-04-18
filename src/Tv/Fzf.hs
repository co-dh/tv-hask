{-
  Fzf: helpers for running fzf picker
  Suspends terminal, spawns fzf, returns selection
  In testMode, returns first value without spawning fzf

  Literal port of Tc/Tc/Fzf.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Fzf
  ( fzfCore
  , fzf
  , fzfIdx
  , parseSel
  , cmdMode
  , gatePoll
  ) where

import Control.Concurrent (forkIO, threadDelay)
import Control.Monad (unless)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.List (foldl')
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Environment (lookupEnv)
import System.IO (hClose, hFlush)
import System.Process
  ( CreateProcess(..), StdStream(..), proc
  , createProcess, waitForProcess
  )
import Text.Read (readMaybe)

import Tv.CmdConfig (CmdCache)
import qualified Tv.CmdConfig as CmdConfig
import Tv.Types (ViewKind, toString, headD)
import qualified Tv.Term as Term

-- | Build fzf argv: popup geometry fitted to content, prepended to caller opts.
-- inTmux picks between --tmux=bottom popup and inline --height; width floors 50
-- (typing room), caps 120; height caps 15. visLines strips the hidden index field
-- when --with-nth=2 is set so measurement matches what fzf actually renders.
-- Width also accounts for --header=/--prompt= strings — the filter prompt (\)
-- uses a 70-char header that got truncated when all input rows were short.
popupArgs :: Bool -> Vector Text -> Text -> Vector Text
popupArgs inTmux opts input =
  let lines_ = filter (not . T.null) (T.splitOn "\n" input)
      popupH = min (length lines_ + 2) 15
      visLines = if V.any (T.isPrefixOf "--with-nth=2") opts
        then map (\l -> case T.splitOn "\t" l of
                          _ : rest -> T.intercalate "\t" rest
                          _        -> l) lines_
        else lines_
      rowsW = foldl' (\m l -> max m (T.length l)) 0 visLines
      optFieldW pfx = V.foldl'
        (\m o -> case T.stripPrefix pfx o of
                   Just v  -> max m (T.length v)
                   Nothing -> m) 0 opts
      hdrW    = optFieldW "--header="
      promptW = optFieldW "--prompt="
      maxW    = maximum [rowsW, hdrW, promptW]
      popupW  = min (max (maxW + 4) 50) 120
      baseArgs = if inTmux
        then V.fromList
               [ T.pack ("--tmux=bottom," ++ show popupW ++ "," ++ show popupH)
               , "--layout=reverse", "--exact", "+i" ]
        else V.fromList
               [ T.pack ("--height=" ++ show (popupH + 1))
               , "--layout=reverse", "--exact", "+i" ]
  in baseArgs V.++ opts

-- | Spawn fzf with argv, pipe input to stdin, poll while it runs, return stdout.
-- Background-reads stdout to avoid pipe stall, runs `poll` every 30 ms so the
-- caller can service its socket/re-render while the popup has focus.
runFzf :: [String] -> Text -> IO () -> IO Text
runFzf argList input poll = do
  -- Pipe stderr too so any fzf warnings/errors don't leak onto the
  -- shared tty and corrupt the display. In non-tmux mode fzf renders
  -- inline on the same terminal we write to; a single stray stderr
  -- line is enough to mangle the highlight region.
  (Just hin, Just hout, Just herr, ph) <- createProcess
    (proc "fzf" argList)
      { std_in = CreatePipe, std_out = CreatePipe, std_err = CreatePipe }
  TIO.hPutStr hin input
  hFlush hin
  hClose hin
  done <- newIORef False
  outRef <- newIORef (T.empty :: Text)
  _ <- forkIO $ do
    out <- TIO.hGetContents hout
    writeIORef outRef out
    writeIORef done True
  -- Drain stderr in the background so the pipe doesn't fill up and
  -- block fzf. We don't surface errors; they're dropped on the floor,
  -- same as the pre-pipe behavior of inheriting stderr and letting the
  -- user see them — minus the display corruption.
  _ <- forkIO $ do
    _ <- TIO.hGetContents herr
    pure ()
  let loop = do
        d <- readIORef done
        unless d $ do
          poll
          threadDelay 30000  -- 30 ms
          loop
  loop
  out <- readIORef outRef
  _ <- waitForProcess ph
  pure out

-- | Suppress a live-preview poll outside of tmux. When fzf runs in
-- tmux we get a popup pane, so callers can safely re-render the main
-- terminal from their @poll@ callback. Without tmux, fzf takes over
-- the same tty with @--height=N@ inline; any 'Term.present' issued by
-- the callback would interleave with fzf's own redraws and scramble
-- the display. See @/bugfix@ — pressing @/@ without tmux produced a
-- junk screen because the @search.preview@ socket message triggered a
-- re-render on top of fzf.
gatePoll :: Bool -> IO () -> IO ()
gatePoll inTmux poll = if inTmux then poll else pure ()

-- | Core fzf: testMode returns first line, else spawn fzf
-- Uses --tmux popup if in tmux (keeps table visible), otherwise compact at bottom
-- poll: optional callback invoked in loop while fzf runs. Automatically
-- muted outside tmux to avoid painting over fzf's inline UI.
fzfCore :: Bool -> Vector Text -> Text -> IO () -> IO Text
fzfCore tm opts input poll = do
  if tm
    then pure (headD "" (filter (not . T.null) (T.splitOn "\n" input)))
    else do
      inTmux <- fmap (maybe False (const True)) $ lookupEnv "TMUX"
      let argList = map T.unpack (V.toList (popupArgs inTmux opts input))
      unless inTmux Term.shutdown
      out <- runFzf argList input (gatePoll inTmux poll)
      -- Clear after re-init: fzf inline renders below termbox area, leaving residue
      unless inTmux $ do
        _ <- Term.init
        Term.clear
        Term.present
      pure (T.strip out)

-- | Single select: returns none if empty/cancelled
fzf :: Bool -> Vector Text -> Text -> IO (Maybe Text)
fzf tm opts input = do
  out <- fzfCore tm opts input (pure ())
  pure (if T.null out then Nothing else Just out)

-- | Index select: testMode returns 0
fzfIdx :: Bool -> Vector Text -> Vector Text -> IO (Maybe Int)
fzfIdx tm opts items = do
  if tm
    then pure (if V.null items then Nothing else Just 0)
    else do
      let numbered = V.imap (\i s -> T.pack (show i) <> "\t" <> s) items
          input = T.intercalate "\n" (V.toList numbered)
      out <- fzfCore tm (V.fromList ["--with-nth=2.."] V.++ opts) input (pure ())
      if T.null out
        then pure Nothing
        else case T.splitOn "\t" out of
          []    -> pure Nothing
          (h:_) -> pure (readMaybe (T.unpack h))

-- | Build aligned menu items: "handler | ctx | key | label" with padding
flatItems :: CmdCache -> ViewKind -> Vector Text
flatItems cc vk =
  let items = CmdConfig.menuItems cc (toString vk)
      (maxH, maxX, maxK) = V.foldl
        (\(mh, mx, mk) (h, x, k, _) ->
           (max mh (T.length h), max mx (T.length x), max mk (T.length k)))
        (0, 0, 0) items
  in V.map (\(handler, ctx_, key_, label_) ->
    let hp = handler <> T.replicate (maxH - T.length handler) " "
        xp = ctx_   <> T.replicate (maxX - T.length ctx_) " "
        kp = key_   <> T.replicate (maxK - T.length key_) " "
    in hp <> " | " <> xp <> " | " <> kp <> " | " <> label_) items

-- | Parse flat selection: extract handler name before first |
--
-- >>> parseSel "plot.area    | cg |   | Plot: area chart"
-- Just "plot.area"
-- >>> parseSel "sort.asc     | c  | [ | Sort ascending"
-- Just "sort.asc"
-- >>> parseSel ""
-- Nothing
parseSel :: Text -> Maybe Text
parseSel sel =
  let h = T.stripEnd (headD "" (T.splitOn " | " sel))
  in if T.null h then Nothing else Just h

-- | Command mode: space -> flat fzf menu -> return handler name.
-- tm forwards the app's testMode to fzfCore. Hardcoding False here made
-- every `-c " "` test spawn real fzf, which grabs /dev/tty and blocks
-- the user's terminal during `cabal test`.
cmdMode :: Bool -> CmdCache -> ViewKind -> IO () -> IO (Maybe Text)
cmdMode tm cc vk poll = do
  let items = flatItems cc vk
  if V.null items
    then pure Nothing
    else do
      let input = T.intercalate "\n" (V.toList items)
          opts  = V.fromList ["--prompt=cmd "]
      out <- fzfCore tm opts input poll
      if T.null out
        then pure Nothing
        else pure $ parseSel out
