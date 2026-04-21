{-
  Fuzzy picker — in-process replacement for the fzf CLI.

  Exposes the same API shape callers used before (@fzfCore@, @fzf@,
  @fzfIdx@, @cmdMode@, @parseSel@, @gatePoll@, @flatItems@) but runs the
  picker inside the Haskell process via 'Tv.Fzf.Picker'. 'fzfCoreLive' is
  a strict superset that adds an @onFocus@ callback, so live previews
  (filter search, theme preview) no longer need the socat+script shim
  that tunnelled fzf's @--bind=focus:execute-silent@ back through the tv
  socket.

  In testMode each call returns a deterministic pick without touching the
  tty — same contract the old @fzfCore tm=True@ branch provided.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Fzf
  ( fzfCore
  , fzfCoreLive
  , fzf
  , fzfIdx
  , parseSel
  , cmdMode
  , gatePoll
  , flatItems
  ) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified System.Environment as Env
import Text.Read (readMaybe)

import Tv.CmdConfig (CmdCache)
import qualified Tv.CmdConfig as CmdConfig
import qualified Tv.Fzf.Picker as Picker
import Tv.Types (ViewKind, toString, headD)

-- | Extract @--prompt=X@ / @--header=X@ / @--print-query@ /
-- @--with-nth=2..@ flags from the caller's opts vector. Unknown flags
-- are ignored (fzf had many; we only implemented what tv used).
parseOpts :: Vector Text -> Picker.PickerOpts
parseOpts argv =
  let get pfx = firstMatch pfx
      hasFlag f = V.any (== f) argv
  in Picker.defaultOpts
       { Picker.prompt     = maybe "> " id (get "--prompt=")
       , Picker.header     = maybe ""   id (get "--header=")
       , Picker.printQuery = hasFlag "--print-query"
       , Picker.withNth    = V.any (T.isPrefixOf "--with-nth=") argv
       }
  where
    firstMatch :: Text -> Maybe Text
    firstMatch pfx = V.foldr
      (\arg acc -> case T.stripPrefix pfx arg of
                     Just v  -> Just v
                     Nothing -> acc) Nothing argv

-- | Core picker. Splits @input@ on newlines into items; returns the raw
-- selected line (with tab-prefix preserved) or empty on cancel.
-- @poll@ is invoked while waiting for keys.
fzfCore :: Bool -> Vector Text -> Text -> IO () -> IO Text
fzfCore tm opts input pollCB = fzfCoreLive tm opts input pollCB (\_ _ -> pure ())

-- | Live-preview variant. @onFocus@ is called each time the highlighted
-- row changes, with (item index in the input, raw line).
--
-- The picker overlays the existing TUI (no terminal teardown/reinit).
-- 'Term.present' from within the picker paints the popup; on exit the
-- cell buffer is cleared so the caller's next redraw repaints the
-- underlying view cleanly.
fzfCoreLive
  :: Bool -> Vector Text -> Text -> IO () -> (Int -> Text -> IO ()) -> IO Text
fzfCoreLive tm opts input pollCB onFocus_ = do
  let allLines = filter (not . T.null) (T.splitOn "\n" input)
  if tm
    then pure (headD "" allLines)
    else do
      let po = (parseOpts opts)
            { Picker.items   = V.fromList allLines
            , Picker.poll    = pollCB
            , Picker.onFocus = onFocus_
            }
      out <- Picker.runPicker po
      pure (T.strip out)

-- | Single-select wrapper: returns 'Nothing' on cancel / empty result.
fzf :: Bool -> Vector Text -> Text -> IO (Maybe Text)
fzf tm opts input = do
  out <- fzfCore tm opts input (pure ())
  pure (if T.null out then Nothing else Just out)

-- | Index select: items are shown as-is, but selection returns the 0-based
-- index into @items@ (or 'Nothing' on cancel).
fzfIdx :: Bool -> Vector Text -> Vector Text -> IO (Maybe Int)
fzfIdx tm opts items_ =
  if tm
    then pure (if V.null items_ then Nothing else Just 0)
    else do
      let numbered = V.imap (\i s -> T.pack (show i) <> "\t" <> s) items_
          input    = T.intercalate "\n" (V.toList numbered)
      out <- fzfCore tm (V.fromList ["--with-nth=2.."] V.++ opts) input (pure ())
      if T.null out
        then pure Nothing
        else case T.splitOn "\t" out of
          []    -> pure Nothing
          (h:_) -> pure (readMaybe (T.unpack h))

-- | Preserve old API name: the picker is always safe to poll during, so
-- the "mute when not in tmux" gating is no longer necessary. Keep the
-- function for callers and the regression test.
gatePoll :: Bool -> IO () -> IO ()
gatePoll _ pollCB = pollCB

-- | Build aligned menu items: @"handler | ctx | key | label"@ with padding.
flatItems :: CmdCache -> ViewKind -> Vector Text
flatItems cc vk =
  let items_ = CmdConfig.menuItems cc (toString vk)
      (maxH, maxX, maxK) = V.foldl
        (\(mh, mx, mk) (h, x, k, _) ->
           (max mh (T.length h), max mx (T.length x), max mk (T.length k)))
        (0, 0, 0) items_
  in V.map (\(handler, ctx_, key_, label_) ->
    let hp = handler <> T.replicate (maxH - T.length handler) " "
        xp = ctx_   <> T.replicate (maxX - T.length ctx_) " "
        kp = key_   <> T.replicate (maxK - T.length key_) " "
    in hp <> " | " <> xp <> " | " <> kp <> " | " <> label_) items_

-- | Parse flat selection: extract handler name before first @|@.
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

-- | Command mode: space → flat menu → return handler name. In testMode,
-- @TV_CMD@ (if set) overrides to a specific handler so plot e2e can drive
-- commands without rebinding keys.
cmdMode :: Bool -> CmdCache -> ViewKind -> IO () -> IO (Maybe Text)
cmdMode tm cc vk pollCB = do
  override <- if tm then lookupTvCmd else pure Nothing
  case override of
    Just s | not (T.null s) -> pure (Just s)
    _ -> do
      let items_ = flatItems cc vk
      if V.null items_
        then pure Nothing
        else do
          let input = T.intercalate "\n" (V.toList items_)
              opts  = V.fromList ["--prompt=cmd "]
          out <- fzfCore tm opts input pollCB
          if T.null out then pure Nothing else pure (parseSel out)
  where
    lookupTvCmd = do
      m <- Env.lookupEnv "TV_CMD"
      pure $ case m of
        Just s | not (null s) -> Just (T.pack s)
        _                     -> Nothing
