{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StrictData #-}
{-# LANGUAGE TemplateHaskell #-}
module Tv.App.Types
  ( -- * State
    AppState(..)
  , precL
  , Action(..)
  , HandlerFn
    -- * State operations
  , resetVS
  , withStk
  , errAction
  , tryStk
  , stackIO
  , runViewEffect
  , viewUp
    -- * Handler combinators
  , onStk
  , argH
  , vuH
  , stkH
  , precSet
  , precAdj
  , heatSet
  ) where

import Control.Exception (SomeException, try)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Maybe (MaybeT(..), runMaybeT, hoistMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8)

import Optics.Core (Lens', (%), (&), (.~), (^.), over, set)
import Optics.TH (makeFieldLabelsNoPrefix)

import Data.HashMap.Strict (HashMap)
import Tv.CmdConfig (CmdInfo(..), CmdCache)
import qualified Tv.Data.DuckDB.Table as AdbcTable
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Nav as Nav
import qualified Tv.Render as Render
import Tv.Render (ViewState, errorPopup)

import qualified Tv.Freq as Freq
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Theme as Theme
import qualified Tv.Data.DuckDB.Ops as Ops
import Tv.Types
  ( Cmd(..)
  , ColCache
  , Effect(..)
  , ViewKind(..)
  , joinWith
  )
import qualified Tv.Log as Log
import Tv.View (View(..), ViewStack(..))
import qualified Tv.View as View

-- Action/HandlerFn must precede AppState so TH splice sees them
data Action = ActQuit | ActUnhandled | ActOk AppState

type HandlerFn = AppState -> CmdInfo -> Text -> IO Action

data AppState = AppState
  { stk         :: ViewStack AdbcTable
  , vs          :: ViewState
  , theme       :: Theme.State
  , info        :: Bool
  , prevScroll  :: Int
  , heatMode    :: Word8
  , sparklines  :: Vector Text
  , statusCache :: ColCache Text Text
  , aggCache    :: StatusAgg.Cache
  , cmdCache    :: CmdCache
  , handlers    :: HashMap Cmd HandlerFn
  , testMode    :: Bool
  , noSign      :: Bool
  }
makeFieldLabelsNoPrefix ''AppState

precL :: Lens' AppState Int
precL = #stk % #hd % #prec

-- State operations --

resetVS :: AppState -> AppState
resetVS a = a & #vs .~ Render.defVS & #sparklines .~ V.empty

withStk :: AppState -> CmdInfo -> ViewStack AdbcTable -> AppState
withStk a ci s' =
  a & #stk .~ s'
    & #vs .~ (if ciResets ci then Render.defVS else a ^. #vs)

errAction :: AppState -> SomeException -> IO Action
errAction a e = do
  let msg = T.pack (show e)
  Log.errorLog msg
  errorPopup msg
  pure (ActOk a)

tryStk :: AppState -> CmdInfo -> IO (Maybe (ViewStack AdbcTable)) -> IO Action
tryStk a ci f = do
  r <- try f :: IO (Either SomeException (Maybe (ViewStack AdbcTable)))
  case r of
    Right (Just s') -> pure $ ActOk $ resetVS (withStk a ci s')
    Right Nothing   -> pure (ActOk a)
    Left  e         -> errAction a e

stackIO :: AppState -> IO (ViewStack AdbcTable) -> IO Action
stackIO a f = do
  r <- try f :: IO (Either SomeException (ViewStack AdbcTable))
  case r of
    Right s' -> pure $ ActOk $ resetVS (a & #stk .~ s')
    Left  e  -> errAction a e

runViewEffect :: AppState -> CmdInfo -> View AdbcTable -> Effect -> IO Action
runViewEffect a ci v' e =
  let s    = stk a
      rCur = Nav.cur (Nav.row (View.nav v'))
  in case e of
    EffectNone -> pure $ ActOk $ withStk a ci (View.setCur s v')
    EffectQuit -> pure ActQuit

    EffectFetchMore -> tryStk a ci $ runMaybeT $ do
      tbl' <- MaybeT (AdbcTable.fetchMore (View.tbl s))
      rv   <- hoistMaybe (View.rebuild v' tbl' 0 V.empty rCur)
      pure $ View.setCur s rv

    EffectSort colIdx sels grp asc -> tryStk a ci $ runMaybeT $ do
      tbl' <- liftIO (Ops.modifyTableSort (View.tbl s) colIdx sels grp asc)
      rv   <- hoistMaybe (View.rebuild v' tbl' colIdx V.empty rCur)
      pure $ View.setCur s rv

    EffectExclude cols -> tryStk a ci $ runMaybeT $ do
      tbl' <- liftIO (AdbcTable.excludeCols (View.tbl s) cols)
      let notExcluded = V.filter (not . (`V.elem` cols))
          grp'        = notExcluded (Nav.grp    (View.nav v'))
          hidden'     = notExcluded (Nav.hidden (View.nav v'))
      rv <- hoistMaybe (View.rebuild v' tbl' 0 grp' rCur)
      pure $ View.setCur s (rv & #nav % #hidden .~ hidden')

    EffectFreq colNames -> tryStk a ci $ runMaybeT $ do
      (adbc, totalGroups) <- MaybeT (AdbcTable.freqTable (View.tbl s) colNames)
      fv <- hoistMaybe (View.fromTbl adbc (View.path (View.cur s)) 0 colNames 0)
      pure $ View.push s (fv
        { View.vkind = VkFreqV colNames totalGroups
        , View.disp  = "freq " <> joinWith colNames ","
        })

    EffectFreqFilter cols row -> tryStk a ci $ runMaybeT $ do
      s'   <- hoistMaybe $ case (View.vkind (View.cur s), View.pop s) of
                (VkFreqV _ _, Just p) -> Just p
                _                     -> Nothing
      expr <- liftIO (Freq.filterIO (View.tbl s) cols row)
      tbl' <- MaybeT (AdbcTable.filter (View.tbl s') expr)
      rv   <- hoistMaybe (View.rebuild (View.cur s') tbl' 0 V.empty 0)
      pure $ View.push s' rv

viewUp :: AppState -> CmdInfo -> IO Action
viewUp a ci =
  case View.update (View.cur (stk a)) (ciCmd ci) 20 of
    Just (v', e) -> runViewEffect a ci v' e
    Nothing      -> pure ActUnhandled

-- Handler combinators --

-- | Run an action on the view stack. The most common handler shape.
onStk :: (ViewStack AdbcTable -> IO (Maybe (ViewStack AdbcTable))) -> HandlerFn
onStk f = \a ci _ -> tryStk a ci (f (stk a))

argH
  :: (ViewStack AdbcTable -> IO (ViewStack AdbcTable))
  -> (ViewStack AdbcTable -> Text -> IO (ViewStack AdbcTable))
  -> HandlerFn
argH fzf direct = \a _ arg ->
  stackIO a (if T.null arg then fzf (stk a) else direct (stk a) arg)

vuH :: HandlerFn
vuH = \a ci _ -> viewUp a ci

stkH :: HandlerFn
stkH = \a ci _ ->
  pure $ case View.updateStack (stk a) (ciCmd ci) of
    Just (_, EffectQuit) -> ActQuit
    Just (s', _)         -> ActOk (withStk a ci s')
    Nothing              -> ActUnhandled

precSet :: Int -> HandlerFn
precSet v = \a _ _ -> pure $ ActOk $ set precL v a

precAdj :: Int -> HandlerFn
precAdj delta = \a _ _ ->
  pure $ ActOk $ over precL (\p -> min 17 (max 0 (p + delta))) a

heatSet :: Word8 -> HandlerFn
heatSet v = \a _ _ -> pure $ ActOk $ a & #heatMode .~ v
