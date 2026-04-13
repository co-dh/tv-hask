{-# LANGUAGE OverloadedStrings #-}
-- | Shared test helpers. Mirrors Tc/test/TestUtil.lean where the Lean
-- helpers map onto the Haskell port. The Lean helpers are oriented around
-- spawning the `tv` binary (which we don't do here) — instead, this module
-- provides helpers for building mock TblOps, views, and stacks, and a few
-- pure string helpers used by the ported screen-level tests.
module TestUtil
  ( -- * String helpers (mirrors Lean isContent/contains/footer/header/dataLines)
    isContent
  , contains
  , footer
  , header
  , dataLines
    -- * Mock table builders
  , mockTbl
  , mockTblSized
  , mockNav
  , mockNavFor
  , testView
  , testStack
  , mkGridTbl
  , mkGridTblTy
    -- * Live DuckDB table helpers (for LargeDataSpec)
  , withMemConn
  , rangeCount
  ) where

import Data.Text (Text)
import qualified Data.Text as T
import Data.Char (isAlpha, isDigit, isSpace)
import Data.Vector (Vector)
import qualified Data.Vector as V

import Tv.Types
import Tv.View
import qualified Tv.Data.DuckDB as D
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- ----------------------------------------------------------------------------
-- Pure string helpers (ported from TestUtil.lean)
-- ----------------------------------------------------------------------------

-- | A line "has content" iff it contains at least one alpha/digit char.
isContent :: Text -> Bool
isContent = T.any (\c -> isAlpha c || isDigit c)

-- | Substring check (mirrors Lean's `contains` definition).
contains :: Text -> Text -> Bool
contains s sub = sub `T.isInfixOf` s

-- | (2nd-to-last, last) content-bearing lines.
footer :: Text -> (Text, Text)
footer out =
  let ls = filter isContent (T.splitOn "\n" out)
      n  = length ls
      getD i = if i >= 0 && i < n then ls !! i else ""
  in (getD (n - 2), getD (n - 1))

-- | First content-bearing line, trimmed to last 80 chars.
header :: Text -> Text
header out = case filter isContent (T.splitOn "\n" out) of
  (h : _) -> if T.length h > 80 then T.drop (T.length h - 80) h else h
  _       -> ""

-- | Data lines: drop header + last 2 (tab/status) lines from content-bearing set.
dataLines :: Text -> [Text]
dataLines out =
  let ls = filter isContent (T.splitOn "\n" out)
      n  = length ls
  in take (max 0 (n - 3)) (drop 1 ls)

-- ----------------------------------------------------------------------------
-- Mock tables
-- ----------------------------------------------------------------------------

-- | 5-row, 3-col mock with cell content "r,c". Stub IO fields error unless
-- explicitly swapped. Mirrors PureSpec/RenderSpec mockOps.
mockTbl :: TblOps
mockTbl = mockTblSized 5 3

-- | Mock with configurable dims. cellStr returns "r,c".
mockTblSized :: Int -> Int -> TblOps
mockTblSized nr nc = TblOps
  { _tblNRows      = nr
  , _tblColNames   = V.fromList [T.pack ("c" <> show i) | i <- [0 .. nc - 1]]
  , _tblTotalRows  = nr
  , _tblQueryOps   = V.empty
  , _tblFilter     = \_ -> pure Nothing
  , _tblDistinct   = \_ -> pure V.empty
  , _tblFindRow    = \_ _ _ _ -> pure Nothing
  , _tblRender     = \_ -> pure V.empty
  , _tblGetCols    = \_ _ _ -> pure V.empty
  , _tblColType    = \_ -> CTOther
  , _tblBuildFilter = \_ _ _ _ -> ""
  , _tblFilterPrompt = \_ _ -> ""
  , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
  , _tblCellStr     = \r c -> pure (T.pack (show r <> "," <> show c))
  , _tblFetchMore   = pure Nothing
  , _tblHideCols    = \_ -> pure (mockTblSized nr nc)
  , _tblSortBy      = \_ _ -> pure (mockTblSized nr nc)
  }

-- | Default mock NavState (5x3 table).
mockNav :: NavState
mockNav = mockNavFor mockTbl

mockNavFor :: TblOps -> NavState
mockNavFor t = NavState
  { _nsTbl = t, _nsRow = mkAxis, _nsCol = mkAxis
  , _nsGrp = V.empty, _nsHidden = V.empty
  , _nsDispIdxs = V.enumFromN 0 (V.length ((t ^. tblColNames)))
  , _nsVkind = VTbl, _nsSearch = "", _nsPrecAdj = 0, _nsWidthAdj = 0, _nsHeatMode = 0
  }

testView :: View
testView = mkView mockNav "data/test.csv"

testStack :: ViewStack
testStack = ViewStack testView []

-- ----------------------------------------------------------------------------
-- DuckDB helpers
-- ----------------------------------------------------------------------------

-- | Run action with an in-memory DuckDB connection.
-- DuckDB.connect caps memory_limit to 1GB globally (see DuckDB.hs).
withMemConn :: (D.Conn -> IO a) -> IO a
withMemConn f = do
  c <- D.connect ":memory:"
  r <- f c
  D.disconnect c
  pure r

-- | Sum of chunk sizes from `SELECT range FROM range(N)` — a way to generate
-- N rows on the fly without any external data files.
rangeCount :: D.Conn -> Int -> IO Int
rangeCount c n = do
  r <- D.query c (T.pack ("SELECT range FROM range(" <> show n <> ")"))
  cs <- D.chunks r
  pure (sum (map D.chunkSize cs))

-- | Build a read-only TblOps from an explicit header + rows grid. All
-- columns default to 'CTStr'. Useful for Meta/Transpose/Diff/Join tests.
mkGridTbl :: [Text] -> [[Text]] -> TblOps
mkGridTbl cols rows = mkGridTblTy cols rows (const CTStr)

-- | Like 'mkGridTbl' but with a per-column type function.
mkGridTblTy :: [Text] -> [[Text]] -> (Int -> ColType) -> TblOps
mkGridTblTy cols rows0 ty =
  let cv = V.fromList cols
      rv = V.fromList (map V.fromList rows0)
      nr = V.length rv
      nc = V.length cv
      ops = TblOps
        { _tblNRows       = nr
        , _tblColNames    = cv
        , _tblTotalRows   = nr
        , _tblQueryOps    = V.empty
        , _tblFilter      = \_ -> pure Nothing
        , _tblDistinct    = \_ -> pure V.empty
        , _tblFindRow     = \_ _ _ _ -> pure Nothing
        , _tblRender      = \_ -> pure V.empty
        , _tblGetCols     = \_ _ _ -> pure V.empty
        , _tblColType     = ty
        , _tblBuildFilter = \_ _ _ _ -> T.empty
        , _tblFilterPrompt = \_ _ -> T.empty
        , _tblPlotExport  = \_ _ _ _ _ _ -> pure Nothing
        , _tblCellStr     = \r c ->
            if r < 0 || r >= nr || c < 0 || c >= nc
              then pure T.empty
              else pure ((rv V.! r) V.! c)
        , _tblFetchMore   = pure Nothing
        , _tblHideCols    = \_ -> pure ops
        , _tblSortBy      = \_ _ -> pure ops
        }
  in ops
