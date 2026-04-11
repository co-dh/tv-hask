{-# LANGUAGE ScopedTypeVariables #-}
-- | Fzf: subprocess integration for fuzzy selection.  Mirrors Tc/Tc/Fzf.lean.
-- `fzfCore` spawns fzf, streams the caller's input on stdin, and reads the
-- chosen line from stdout.  `cmdMode` builds the "space menu" from
-- CmdConfig.menuItems and returns the chosen handler name.
--
-- Brick owns the terminal, so callers must wrap fzfCore in
-- `Brick.suspendAndResume` so the terminal state is handed back to fzf for
-- the duration of the picker and re-grabbed afterwards.
module Tv.Fzf where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import System.IO (hClose, hFlush, hGetContents, hPutStr)
import System.Process
  ( CreateProcess(..), StdStream(..), createProcess, proc, waitForProcess )
import System.Environment (lookupEnv)
import Control.Exception (try, SomeException)
import qualified Tv.CmdConfig as CC

-- | Extract the first field (handler name) from an aligned flat-selection line:
--   "plot.area    | cg |   | Plot: area chart"  →  Just "plot.area"
parseFlatSel :: Text -> Maybe Text
parseFlatSel s
  | T.null s = Nothing
  | otherwise = case T.splitOn "|" s of
      (h:_) -> let h' = T.strip h in if T.null h' then Nothing else Just h'
      _ -> Nothing

-- | Run fzf with @opts@ as extra CLI flags and @input@ piped on stdin.
-- Returns the selected line (trimmed). Returns empty text on cancel or error.
-- Uses --tmux popup if $TMUX is set, else --height inline picker.
fzfCore :: [Text] -> Text -> IO Text
fzfCore opts input = do
  tmux <- lookupEnv "TMUX"
  let lns = filter (not . T.null) (T.splitOn "\n" input)
      popupH = min 15 (length lns + 2)
      withNth2 = any ("--with-nth=2" `T.isPrefixOf`) opts
      visLines =
        if withNth2
          then map (\l -> case T.splitOn "\t" l of _:rest -> T.intercalate "\t" rest; _ -> l) lns
          else lns
      maxW = foldr (max . T.length) 0 visLines
      popupW = min 80 (max 50 (maxW + 4))
      baseArgs = case tmux of
        Just _  -> ["--tmux=bottom," <> tshow popupW <> "," <> tshow popupH
                   , "--layout=reverse", "--exact", "+i"]
        Nothing -> ["--height=" <> tshow (popupH + 1)
                   , "--layout=reverse", "--exact", "+i"]
      args = map T.unpack (baseArgs ++ opts)
  r <- try $ do
    let cp = (proc "fzf" args)
               { std_in = CreatePipe, std_out = CreatePipe, std_err = Inherit }
    (Just hin, Just hout, _, ph) <- createProcess cp
    hPutStr hin (T.unpack input)
    hFlush hin
    hClose hin
    out <- hGetContents hout
    _ <- length out `seq` waitForProcess ph
    pure (T.strip (T.pack out))
  case r of
    Right t -> pure t
    Left (_ :: SomeException) -> pure ""  -- fzf missing or cancelled
  where tshow = T.pack . show

-- | Single-select wrapper; returns Nothing on empty/cancel.
fzf :: [Text] -> Text -> IO (Maybe Text)
fzf opts input = do
  out <- fzfCore opts input
  pure (if T.null out then Nothing else Just out)

-- | Indexed select over a list of items. Prefixes each line with "i\t",
-- hides the index from the display with --with-nth=2.., parses the index
-- out of the selection.
fzfIdx :: [Text] -> [Text] -> IO (Maybe Int)
fzfIdx opts items = do
  let numbered = [ T.pack (show i) <> "\t" <> s | (i, s) <- zip [0 :: Int ..] items ]
  out <- fzfCore ("--with-nth=2.." : opts) (T.intercalate "\n" numbered)
  if T.null out then pure Nothing
  else case T.splitOn "\t" out of
    (n:_) -> case reads (T.unpack n) of [(i, "")] -> pure (Just i); _ -> pure Nothing
    _ -> pure Nothing

-- | Build aligned menu items from CmdConfig for a given view context.
-- Each line is @"handler | ctx | key | label"@ with columns padded so the
-- pipes line up in fzf.  Used by 'cmdMode' for the space-bar menu.
flatItems :: Text -> IO [Text]
flatItems vctx = do
  items <- CC.menuItems vctx
  let (maxH, maxX, maxK) = foldr step (0, 0, 0) items
      step (h, x, k, _) (mh, mx, mk) =
        (max mh (T.length h), max mx (T.length x), max mk (T.length k))
      pad n t = t <> T.replicate (max 0 (n - T.length t)) " "
  pure [ pad maxH h <> " | " <> pad maxX x <> " | " <> pad maxK k <> " | " <> lbl
       | (h, x, k, lbl) <- items ]

-- | Space menu: show fzf picker over all commands visible in @vctx@.
-- Returns the handler-name string (first "|" field) of the chosen row, or
-- Nothing on cancel / empty menu.
cmdMode :: Text -> IO (Maybe Text)
cmdMode vctx = do
  items <- flatItems vctx
  if null items then pure Nothing
  else do
    out <- fzfCore ["--prompt=cmd "] (T.intercalate "\n" items)
    pure $ if T.null out then Nothing else parseFlatSel out
