{-
  Derive: add computed column via PRQL derive.
  Press '=' -> fzf prompt with column names as hints -> requery with derive -> push view.

  Literal port of Tc/Tc/Derive.lean.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Derive
  ( samples
  , colTypeStr
  , colHints
  , parseDerive
  , runWith
  , run
  ) where

import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import Optics.Core ((&), (.~))

import qualified Tv.Fzf as Fzf
import qualified Tv.Nav as Nav
import Tv.Types (ColType (..), Op (..), colTypeStr)
import qualified Tv.Types as TblOps
import qualified Tv.Util as Log
import qualified Tv.View as View
import Tv.View (ViewStack)

import qualified Tv.Data.ADBC.Prql as Prql
import qualified Tv.Data.ADBC.Table as Table
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Data.ADBC.Ops ()  -- TblOps AdbcTable instance

-- | Sample PRQL expressions by column type, showing name = expr format
samples :: Text -> ColType -> Text
samples col ty = case ty of
  ColTypeInt     -> numSamples
  ColTypeFloat   -> numSamples
  ColTypeDecimal -> numSamples
  ColTypeStr ->
    -- f-string uses literal braces (PRQL syntax)
    "d = " <> col <> " != null | d = f\"{col}-{other}\" | d = " <> col <> " | text.upper"
  ColTypeDate ->
    "d = " <> col <> " != null | d = " <> col <> " | date.year | d = " <> col <> " - @2024-01-01"
  ColTypeTime ->
    timeSamples
  ColTypeTimestamp ->
    timeSamples
  ColTypeBool ->
    "d = " <> col <> " != null | d = " <> col <> " == false | d = !" <> col
  ColTypeOther ->
    "d = " <> col <> " != null | d = " <> col <> " > 0 | d = f\"{col}\""
  where
    numSamples =
      "d = " <> col <> " * 2 | d = " <> col <> " != null | d = math.round 2 " <> col
    timeSamples =
      "d = " <> col <> " != null | d = " <> col <> " | date.hour | d = " <> col <> " | date.minute"


-- | Build "col : type" lines with aligned ":"
colHints :: Vector Text -> Vector ColType -> Text
colHints names types =
  let maxLen = V.foldl (\mx n -> max mx (T.length n)) 0 names
      line i n =
        let pad = T.replicate (maxLen - T.length n) " "
            ty  = fromMaybe ColTypeOther (types V.!? i)
        in n <> pad <> " : " <> colTypeStr ty
  in T.intercalate "\n" (V.toList (V.imap line names))

-- | Parse "name = expr" format. Returns Nothing if no "=" found.
parseDerive :: Text -> Maybe (Text, Text)
parseDerive input =
  case T.splitOn " = " input of
    []           -> Nothing
    (name : rest) ->
      let n = T.strip name
          e = T.strip (T.intercalate " = " rest)  -- rejoin in case expr contains " = "
      in if T.null n || T.null e then Nothing else Just (n, e)

-- | Derive column from expression (no fzf). Called by socket/dispatch and by `run` after fzf.
runWith :: ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable)
runWith s input = case parseDerive input of
  Nothing -> pure s
  Just (name, expr) -> do
    let q = Prql.pipe (Table.query (View.tbl s)) (OpDerive (V.singleton (name, expr)))
    Log.write "derive" (Prql.queryRender q)
    mTbl <- Table.requery q (Table.totalRows (View.tbl s))
    case mTbl of
      Nothing -> pure s
      Just tbl' ->
        let nCols = V.length (TblOps.colNames tbl')
            curV  = View.cur s
            grp_  = Nav.grp (View.nav curV)
        in case View.rebuild curV tbl' (nCols - 1) grp_ 0 of
             Nothing -> pure s
             Just v  -> pure (View.push s (v & #disp .~ ("=" <> name)))

-- | Prompt for name = expr via fzf, then derive.
run :: ViewStack AdbcTable -> IO (ViewStack AdbcTable)
run s = do
  let nav     = View.nav (View.cur s)
      names   = Nav.colNames nav
      curName = Nav.curColName nav
      typ     = Nav.curColType nav
      header  = "name = expr\n" <> samples curName typ
      hint    = colHints names (Table.colTypes (View.tbl s))
  mRaw <- Fzf.fzf
            (V.fromList ["--print-query", "--prompt=derive: ", "--header=" <> header])
            hint
  case mRaw of
    Nothing  -> pure s
    Just raw -> runWith s (T.strip raw)
