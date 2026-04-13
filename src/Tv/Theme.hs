-- | Theme: CSV-based color themes → brick AttrMap.
-- Themes are loaded from theme.csv (next to binary, CWD, or builtin).
-- Live preview via socket + fzf picker.
module Tv.Theme where

import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.IORef
import Data.Word (Word32)
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.IO.Unsafe (unsafePerformIO)
import System.Directory (doesFileExist, getXdgDirectory, XdgDirectory(..))
import System.Environment (getExecutablePath, lookupEnv)
import System.FilePath (takeDirectory, (</>))
import qualified Graphics.Vty as Vty
import qualified Brick.AttrMap as Brick
import Brick.Util (fg, bg, on)
import Tv.Eff (Eff, IOE, (:>), liftIO)

-- | Style indices (matching Lean's constants)
data Style = SCursor | SSelRow | SSelColCurRow | SSelCol | SCurRow | SCurCol
           | SDefault | SHeader | SGroup | SStatus | SStatusDim | SBar | SBarDim
           | SError | SErrorDim | SHint
  deriving (Eq, Ord, Show, Enum, Bounded)

styleNames :: [Text]
styleNames =
  [ "cursor", "selRow", "selColCurRow", "selCol"
  , "curRow", "curCol", "default", "header", "group"
  , "status", "statusDim", "bar", "barDim", "error", "errorDim", "hint" ]

-- | (fg, bg) per style. 'Nothing' means "keep terminal default" — do
-- not call 'withForeColor' / 'withBackColor' on the Vty.Attr. This
-- matters because Vty's Color type has no default constructor;
-- encoding default via ISOColor 9 paints bright red everywhere.
type StylePair = (Maybe Vty.Color, Maybe Vty.Color)

data ThemeState = ThemeState
  { tsStyles   :: !(Vector StylePair)
  , tsThemeIdx :: !Int
  } deriving (Show)

-- | Available themes (must match theme.csv)
themes :: Vector (Text, Text)
themes = V.fromList
  [ ("default","dark"), ("default","light"), ("ansi","dark"), ("ansi","light")
  , ("nord","dark"), ("nord","light"), ("dracula","dark"), ("dracula","light")
  , ("gruvbox","dark"), ("gruvbox","light"), ("monokai","dark"), ("monokai","light") ]

themeName :: Int -> Text
themeName idx = let (t, v) = themes V.! idx in t <> " (" <> v <> ")"

-- | Parse color name to termbox256 index. Mirrors Lean Term.parseColor exactly,
-- so tests can assert specific indices. 0 = TB_DEFAULT.
-- Layout: 0 default; 1..8 ANSI; 9..15 bright; 16..231 RGB cube (16+36R+6G+B); 232..255 gray.
parseColorIx :: Text -> Int
parseColorIx = \case
  "default"  -> 0
  "red"      -> 1
  "green"    -> 2
  "yellow"   -> 3
  "blue"     -> 4
  "magenta"  -> 5
  "cyan"     -> 6
  "white"    -> 7
  "brBlack"  -> 8
  "brRed"    -> 9
  "brGreen"  -> 10
  "brYellow" -> 11
  "brBlue"   -> 12
  "brMagenta"-> 13
  "brCyan"   -> 14
  "brWhite"  -> 15
  "black"    -> 16  -- cube black, since 0 = TB_DEFAULT
  t | Just rgb <- T.stripPrefix "rgb" t
    , T.length rgb == 3
    , Just (r,g,b) <- three rgb
    , all (\x -> x >= 0 && x <= 5) [r,g,b]
    -> 16 + 36 * r + 6 * g + b
    | Just n <- T.stripPrefix "gray" t
    , Just g <- readMaybe (T.unpack n)
    , g >= 0 && g <= 23
    -> 232 + g
    | otherwise -> 0
  where
    readMaybe s = case reads s of [(v,"")] -> Just v; _ -> Nothing
    digit c | c >= '0' && c <= '9' = Just (fromEnum c - fromEnum '0')
            | otherwise = Nothing
    three t = case T.unpack t of
      [a,b,c] -> (,,) <$> digit a <*> digit b <*> digit c
      _ -> Nothing

-- | Parse color name to a Vty.Color. 'Nothing' = use terminal default
-- (do not set fg/bg on the attr). Vty has no explicit default-color
-- constructor; using ISOColor 9 would paint bright red.
parseColor :: Text -> Maybe Vty.Color
parseColor "default"   = Nothing
parseColor "black"     = Just Vty.black;     parseColor "red"       = Just Vty.red
parseColor "green"     = Just Vty.green;     parseColor "yellow"    = Just Vty.yellow
parseColor "blue"      = Just Vty.blue;      parseColor "magenta"   = Just Vty.magenta
parseColor "cyan"      = Just Vty.cyan;      parseColor "white"     = Just Vty.white
parseColor "brBlack"   = Just Vty.brightBlack;   parseColor "brRed"     = Just Vty.brightRed
parseColor "brGreen"   = Just Vty.brightGreen;   parseColor "brYellow"  = Just Vty.brightYellow
parseColor "brBlue"    = Just Vty.brightBlue;    parseColor "brMagenta" = Just Vty.brightMagenta
parseColor "brCyan"    = Just Vty.brightCyan;    parseColor "brWhite"   = Just Vty.brightWhite
parseColor t
  | Just rgb <- T.stripPrefix "rgb" t, T.length rgb == 3
  , [r,g,b] <- map (fromIntegral . subtract 48 . fromEnum) (T.unpack rgb)
  = Just (Vty.rgbColor (r * 51 :: Int) (g * 51) (b * 51))
  | Just n <- T.stripPrefix "gray" t, Just g <- readMaybe (T.unpack n)
  = Just (Vty.rgbColor (g * 11 :: Int) (g * 11) (g * 11))
  | otherwise = Nothing
  where readMaybe s = case reads s of [(v,"")] -> Just v; _ -> Nothing

-- | Default dark theme
defaultDark :: Vector StylePair
defaultDark = V.fromList
  [ (parseColor "black", parseColor "brWhite")       -- cursor
  , (parseColor "black", parseColor "rgb354")         -- selRow
  , (parseColor "black", parseColor "rgb435")         -- selColCurRow
  , (parseColor "brMagenta", parseColor "default")    -- selCol
  , (parseColor "default", parseColor "gray2")        -- curRow
  , (parseColor "default", parseColor "gray6")        -- curCol
  , (parseColor "default", parseColor "default")      -- default
  , (parseColor "green", parseColor "rgb112")         -- header
  , (parseColor "default", parseColor "gray5")        -- group
  , (parseColor "cyan", parseColor "default")         -- status
  , (parseColor "brBlack", parseColor "default")      -- statusDim
  , (parseColor "white", parseColor "blue")           -- bar
  , (parseColor "brBlack", parseColor "blue")         -- barDim
  , (parseColor "white", parseColor "red")            -- error
  , (parseColor "brBlack", parseColor "red")          -- errorDim
  , (parseColor "black", parseColor "yellow")         -- hint
  ]

-- | Global styles ref (for errorPopup/statusMsg access without parameter threading)
{-# NOINLINE stylesRef #-}
stylesRef :: IORef (Vector StylePair)
stylesRef = unsafePerformIO $ newIORef defaultDark

getStyles :: IOE :> es => Eff es (Vector StylePair)
getStyles = liftIO (readIORef stylesRef)

-- | Detect terminal background: dark (True) or light (False)
isDark :: IOE :> es => Eff es Bool
isDark = do
  mfgbg <- liftIO (lookupEnv "COLORFGBG")
  pure $ case mfgbg of
    Just s -> case reads (reverse $ takeWhile (/= ';') $ reverse s) of
      [(bg, "")] -> (bg :: Int) < 7
      _ -> True
    Nothing -> True

-- | Builtin CSV fallback (embedded at compile time via TH or just a constant)
builtinCsv :: Text
builtinCsv = ""  -- TODO: embed theme.csv via file-embed or TH

-- | Load theme.csv: next to binary, then CWD, then builtin
loadCsv :: IOE :> es => Eff es Text
loadCsv = do
  exePath <- liftIO getExecutablePath
  let binDir = takeDirectory exePath
  tryPaths [binDir </> "theme.csv", "theme.csv"]
  where
    tryPaths [] = pure builtinCsv
    tryPaths (p:ps) = do
      ok <- liftIO (doesFileExist p)
      if ok then liftIO (TIO.readFile p) else tryPaths ps

-- | Load theme by name from CSV
loadTheme :: IOE :> es => Text -> Text -> Eff es (Vector StylePair)
loadTheme theme variant = do
  csv <- loadCsv
  let lns = filter (not . T.null) (T.splitOn "\n" csv)
  case lns of
    [] -> pure defaultDark
    (hdr:rows) ->
      let colNms = drop 2 (T.splitOn "," hdr)
          mrow = find (\l -> let cs = T.splitOn "," l in
                    getAt cs 0 == theme && getAt cs 1 == variant) rows
      in case mrow of
        Nothing -> pure defaultDark
        Just r ->
          let cols = T.splitOn "," r
              go styles i = case lookup (getAt colNms i) styleMap of
                Nothing -> styles
                Just idx ->
                  let cell = T.splitOn " " (getAt cols (i + 2))
                      fgc = parseColor (getAt cell 0)
                      bgc = parseColor (getAt cell 1)
                  in styles V.// [(idx, (fgc, bgc))]
          in pure $ foldl go defaultDark [0 .. length colNms - 1]
  where
    getAt xs i = if i < length xs then xs !! i else ""
    styleMap = zip styleNames [0..]
    find p = foldr (\x acc -> if p x then Just x else acc) Nothing

-- | Load theme by index
loadIdx :: IOE :> es => Int -> Eff es (Vector StylePair)
loadIdx idx = let (t, v) = themes V.! idx in loadTheme t v

-- | Apply a StylePair to a base attr, respecting Nothing = "keep default".
stylePairAttr :: StylePair -> Vty.Attr
stylePairAttr (mfg, mbg) =
  let a0 = Vty.defAttr
      a1 = maybe a0 (Vty.withForeColor a0) mfg
      a2 = maybe a1 (Vty.withBackColor a1) mbg
  in a2

-- | Convert styles to brick AttrMap
toAttrMap :: Vector StylePair -> Brick.AttrMap
toAttrMap styles = Brick.attrMap Vty.defAttr
  [ (Brick.attrName (T.unpack n), stylePairAttr (styles V.! i))
  | (i, n) <- zip [0..] styleNames
  , i < V.length styles ]

-- | Initialize theme state
initTheme :: IOE :> es => Eff es ThemeState
initTheme = do
  dark <- isDark
  let v = if dark then "dark" else "light"
  styles <- loadTheme "default" v
  liftIO (writeIORef stylesRef styles)
  let idx = maybe 0 id $ V.findIndex (== ("default", v)) themes
  pure $ ThemeState styles idx

-- | Apply theme by index
applyTheme :: IOE :> es => ThemeState -> Int -> Eff es ThemeState
applyTheme ts idx = do
  styles <- loadIdx idx
  liftIO (writeIORef stylesRef styles)
  pure $ ts { tsStyles = styles, tsThemeIdx = idx }
