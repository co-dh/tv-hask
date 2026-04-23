{-
  Theme: CSV-based color themes with fzf picker and live preview via socket.
  Format: theme,variant,cursor,selRow,...  (pivoted: style names are columns, cells are "fg bg")

  Literal port of Tc/Tc/Theme.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.Theme where

import Tv.Prelude
import Control.Exception (SomeException, try)
import Data.List (find)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Environment (getExecutablePath, lookupEnv)
import System.IO.Unsafe (unsafePerformIO)
import Text.Read (readMaybe)


import qualified Tv.Fzf as Fzf
import qualified Tv.Term as Term
import Tv.Types (headD, getD)
import Optics.TH (makeFieldLabelsNoPrefix)

-- | Theme state
data State = State
  { styles   :: Vector Word32
  , themeIdx :: Int
  }
makeFieldLabelsNoPrefix ''State


-- | Style names → index into styles array. Styles 0-8 used by C render, 9+ by Lean UI.
styleNames :: Vector Text
styleNames = V.fromList
  [ "cursor", "selRow", "selColCurRow", "selCol"
  , "curRow", "curCol", "default", "header", "group"
  , "status", "statusDim", "bar", "barDim", "error", "errorDim", "hint"
  , "pickerPanel", "pickerSel", "pickerMatch"
  ]

-- | Style index constants for Lean-side UI (C render uses 0-8 directly)
sStatus, sStatusDim, sBar, sBarDim, sError, sErrorDim, sHint,
  sPickerPanel, sPickerSel, sPickerMatch :: Int
sStatus      = 9
sStatusDim   = 10
sBar         = 11
sBarDim      = 12
sError       = 13
sErrorDim    = 14
sHint        = 15
sPickerPanel = 16
sPickerSel   = 17
sPickerMatch = 18

styleFg :: Vector Word32 -> Int -> Word32
styleFg sty idx = fromMaybe 0 $ sty V.!? (idx * 2)

styleBg :: Vector Word32 -> Int -> Word32
styleBg sty idx = fromMaybe 0 $ sty V.!? (idx * 2 + 1)

parseStyle :: Text -> Maybe Int
parseStyle s = V.findIndex (== s) styleNames

-- | Default dark theme (fallback if CSV fails)
c :: Text -> Word32
c s = Term.parseColor s

defaultDark :: Vector Word32
defaultDark = V.fromList
  [ c "black", c "brWhite"       -- 0: cursor
  , c "black", c "rgb354"        -- 1: selRow
  , c "black", c "rgb435"        -- 2: selColCurRow
  , c "brMagenta", c "default"   -- 3: selCol
  , c "default", c "gray5"       -- 4: curRow (was gray2 — too dark to see)
  , c "default", c "gray8"       -- 5: curCol (was gray6)
  , c "default", c "default"     -- 6: default
  , c "green", c "rgb112"        -- 7: header
  , c "default", c "gray5"       -- 8: group
  , c "cyan", c "default"        -- 9: status
  , c "brBlack", c "default"     -- 10: statusDim
  , c "white", c "blue"          -- 11: bar
  , c "brBlack", c "blue"        -- 12: barDim
  , c "white", c "red"           -- 13: error
  , c "brBlack", c "red"         -- 14: errorDim
  , c "black", c "yellow"        -- 15: hint
  , c "gray21", c "gray2"        -- 16: pickerPanel
  , c "black", c "brYellow"      -- 17: pickerSel
  , c "brYellow", c "default"    -- 18: pickerMatch
  ]

-- | Global styles ref — lets errorPopup/statusMsg access theme without parameter threading
stylesRef :: IORef (Vector Word32)
stylesRef = unsafePerformIO (newIORef defaultDark)
{-# NOINLINE stylesRef #-}

getStyles :: IO (Vector Word32)
getStyles = readIORef stylesRef

-- | Detect terminal background: dark (true) or light (false)
isDark :: IO Bool
isDark = do
  mcfb <- lookupEnv "COLORFGBG"
  case mcfb of
    Just s ->
      case listToMaybe (reverse (T.splitOn ";" $ T.pack s)) >>= (readMaybe . T.unpack) of
        Just bg -> pure (bg < (7 :: Int))
        Nothing -> pure True
    Nothing -> pure True

-- | Available themes (must match theme.csv)
themes :: Vector (Text, Text)
themes = V.fromList
  [ ("default", "dark"), ("default", "light")
  , ("ansi", "dark"), ("ansi", "light")
  , ("nord", "dark"), ("nord", "light")
  , ("dracula", "dark"), ("dracula", "light")
  , ("gruvbox", "dark"), ("gruvbox", "light")
  , ("monokai", "dark"), ("monokai", "light")
  ]

themeName :: Int -> Text
themeName idx =
  let (t, v) = fromMaybe ("default", "dark") $ themes V.!? idx
  in t <> " (" <> v <> ")"

-- | Embedded CSV as compile-time fallback
-- Inline copy of theme.csv (kept in sync with the file on disk).
builtinCsv :: Text
builtinCsv = T.unlines
  [ "theme,variant,cursor,selRow,selColCurRow,selCol,curRow,curCol,default,header,group,status,statusDim,bar,barDim,error,errorDim,hint,pickerPanel,pickerSel,pickerMatch"
  , "default,dark,black brWhite,black rgb354,black rgb435,brMagenta default,default gray2,default gray6,default default,green rgb112,default gray5,cyan default,brBlack default,white blue,brBlack blue,white red,brBlack red,black yellow,gray21 gray2,black brYellow,brYellow default"
  , "default,light,white black,black brCyan,black rgb435,magenta default,default gray20,default gray20,default default,black brWhite,default gray20,cyan default,brBlack default,black white,brBlack white,white red,brBlack red,black yellow,black gray22,white blue,red default"
  , "ansi,dark,black brWhite,black brCyan,black brMagenta,brYellow default,default brBlack,default brBlack,default default,brWhite blue,default brBlack,cyan default,brBlack default,white blue,brBlack blue,white red,brBlack red,black yellow,brWhite rgb001,black brYellow,brYellow default"
  , "ansi,light,brWhite black,black cyan,black magenta,blue default,default white,default white,default default,black white,default white,cyan default,brBlack default,black white,brBlack white,white red,brBlack red,black yellow,black rgb553,white blue,red default"
  , "nord,dark,black rgb234,black rgb232,black rgb435,rgb234 default,default gray4,default gray4,gray21 default,gray21 gray4,default gray4,cyan default,brBlack default,gray21 gray4,brBlack gray4,white red,brBlack red,black yellow,gray22 rgb011,rgb011 rgb344,rgb542 default"
  , "nord,light,gray4 rgb234,gray4 rgb232,gray4 rgb435,rgb234 default,default gray21,default gray21,gray4 default,gray4 gray21,default gray21,gray4 default,brBlack default,gray4 gray21,brBlack gray21,white red,brBlack red,black yellow,gray4 gray22,gray22 rgb345,rgb014 default"
  , "dracula,dark,black rgb524,black rgb325,black rgb435,rgb524 default,default gray6,default gray6,default default,brWhite rgb325,default gray6,cyan default,brBlack default,brWhite rgb325,brBlack rgb325,white red,brBlack red,black yellow,rgb554 rgb122,rgb011 rgb524,rgb554 default"
  , "dracula,light,white rgb325,white rgb524,black rgb435,rgb325 default,default gray18,default gray18,default default,white rgb325,default gray18,rgb325 default,brBlack default,white rgb325,brBlack rgb325,white red,brBlack red,black yellow,black rgb554,rgb554 rgb325,rgb325 default"
  , "gruvbox,dark,black rgb520,black rgb330,black rgb543,rgb520 default,default gray4,default gray4,rgb553 default,rgb553 rgb310,default gray4,rgb553 default,rgb310 default,rgb553 rgb310,rgb310 rgb310,white red,brBlack red,black yellow,rgb553 rgb111,rgb111 rgb531,rgb520 default"
  , "gruvbox,light,rgb310 rgb520,rgb310 rgb543,rgb310 rgb330,red default,default rgb553,default rgb553,rgb310 default,rgb553 rgb310,default rgb553,rgb310 default,rgb553 default,rgb310 rgb553,rgb553 rgb553,white red,brBlack red,black yellow,rgb200 rgb553,rgb553 rgb310,rgb310 default"
  , "monokai,dark,black brYellow,black rgb512,black rgb325,rgb512 default,default gray6,default gray6,default default,brWhite gray6,default gray6,cyan default,brBlack default,white gray6,brBlack gray6,white red,brBlack red,black yellow,rgb554 rgb111,rgb111 rgb541,rgb502 default"
  , "monokai,light,gray6 brYellow,black rgb512,black rgb325,rgb325 default,default gray20,default gray20,default default,white gray6,default gray20,cyan default,brBlack default,white gray6,brBlack gray6,white red,brBlack red,black yellow,gray6 rgb554,rgb554 rgb325,rgb325 default"
  ]

-- | Find theme.csv: next to binary, then CWD, then builtin
loadCsv :: IO Text
loadCsv = do
  bin <- getExecutablePath
  let dir = T.unpack (T.intercalate "/" (init (T.splitOn "/" (T.pack bin))))
      candidates = [dir ++ "/theme.csv", "theme.csv"]
  tryPaths candidates
  where
    tryPaths :: [FilePath] -> IO Text
    tryPaths [] = pure builtinCsv
    tryPaths (p:ps) = do
      r <- try (TIO.readFile p) :: IO (Either SomeException Text)
      case r of
        Right t -> pure t
        Left _  -> tryPaths ps

-- | Load theme by name. CSV is pivoted: columns are style names, cells are "fg bg".
load :: Text -> Text -> IO (Vector Word32)
load theme variant = do
  content <- loadCsv
  let lines_   = filter (not . T.null) (T.splitOn "\n" content)
      header   = T.splitOn "," (headD "" lines_)
      colNames = drop 2 header  -- style names from header
      row      = find (\line ->
                    let cols = T.splitOn "," line
                    in getD cols 0 "" == theme && getD cols 1 "" == variant)
                 (drop 1 lines_)
  case row of
    Nothing -> pure defaultDark
    Just r  -> do
      let cols = T.splitOn "," r
          go sty i
            | i >= length colNames = sty
            | otherwise =
                case parseStyle (getD colNames i "") of
                  Nothing  -> go sty (i + 1)
                  Just idx ->
                    let cell = T.splitOn " " (getD cols (i + 2) "")
                        fg   = Term.parseColor (getD cell 0 "default")
                        bg   = Term.parseColor (getD cell 1 "default")
                        sty' = sty V.// [(idx * 2, fg), (idx * 2 + 1, bg)]
                    in go sty' (i + 1)
      pure (go defaultDark 0)

-- | Load theme by index
loadIdx :: Int -> IO (Vector Word32)
loadIdx idx = do
  let (theme, variant) = fromMaybe ("default", "dark") $ themes V.!? idx
  load theme variant

-- namespace State

stateInit :: IO State
stateInit = do
  dark <- isDark
  let variant = if dark then "dark" else "light"
  sty <- load "default" variant
  writeIORef stylesRef sty
  let idx = fromMaybe 0 $ V.findIndex (== ("default", variant)) themes
  pure (State { styles = sty, themeIdx = idx })

applyIdx :: State -> Int -> IO State
applyIdx s idx = do
  sty <- loadIdx idx
  writeIORef stylesRef sty
  pure (s { styles = sty, themeIdx = idx })

-- end State

-- | Theme picker with live preview.
-- applyAndRender: called with loaded styles when user moves focus (for live re-render).
-- Returns (selectedIdx, styles), or none on cancel.
run :: Bool -> State -> (Vector Word32 -> IO ()) -> IO (Maybe State)
run tm cur applyAndRender = do
  let State{themeIdx = curIdx} = cur
  if tm
    then do
      let idx = (curIdx + 1) `mod` V.length themes
      sty <- loadIdx idx
      pure (Just (State { styles = sty, themeIdx = idx }))
    else do
      let items = V.imap (\i _ -> T.pack (show i) <> "\t" <> themeName i) themes
          onFocus _ line = case T.splitOn "\t" line of
            (hTxt : _) -> maybe (pure ()) (\i -> loadIdx i >>= applyAndRender)
                                (readMaybe (T.unpack hTxt))
            _ -> pure ()
          opts = V.fromList
            [ "--prompt=theme: ", "--with-nth=2..", "--delimiter=\t" ]
      out <- Fzf.fzfCoreLive False opts (T.intercalate "\n" (V.toList items))
               (pure ()) onFocus
      if T.null out
        then pure Nothing
        else maybe (pure Nothing) (fmap Just . applyIdx cur)
                   (listToMaybe (T.splitOn "\t" out) >>= (readMaybe . T.unpack))
