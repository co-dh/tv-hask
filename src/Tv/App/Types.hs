{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
  , domainH
  , domainH'
  , argH
  , vuH
  , stkH
  , precSet
  , precAdj
  ) where

import Control.Exception (SomeException, try)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8)

import Optics.Core (Lens', (%), (&), (.~), (^.), over, set)
import Optics.TH (makeFieldLabelsNoPrefix)

import Tv.CmdConfig (CmdInfo(..))
import qualified Tv.Data.ADBC.Table as AdbcTable
import Tv.Data.ADBC.Table (AdbcTable)
import qualified Tv.Nav as Nav
import qualified Tv.Render as Render
import Tv.Render (ViewState, errorPopup)
import qualified Tv.Runner as Freq
import qualified Tv.StatusAgg as StatusAgg
import qualified Tv.Theme as Theme
import Tv.Types
  ( Cmd(..)
  , Effect(..)
  , ViewKind(..)
  , noEffect
  )
import qualified Tv.Types as TblOps
import qualified Tv.Util as Log
import Tv.View (View(..), ViewStack(..))
import qualified Tv.View as View

data AppState = AppState
  { stk         :: ViewStack AdbcTable
  , vs          :: ViewState
  , theme       :: Theme.State
  , info        :: Bool
  , prevScroll  :: Int
  , heatMode    :: Word8
  , sparklines  :: Vector Text
  , statusCache :: (Text, Text, Text)
  , aggCache    :: StatusAgg.Cache
  }
makeFieldLabelsNoPrefix ''AppState

precL :: Lens' AppState Int
precL = #stk % #hd % #prec

data Action = ActQuit | ActUnhandled | ActOk AppState

type HandlerFn = AppState -> CmdInfo -> Text -> IO Action

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
    Right (Just s') -> pure (ActOk (resetVS (withStk a ci s')))
    Right Nothing   -> pure (ActOk a)
    Left  e         -> errAction a e

stackIO :: AppState -> IO (ViewStack AdbcTable) -> IO Action
stackIO a f = do
  r <- try f :: IO (Either SomeException (ViewStack AdbcTable))
  case r of
    Right s' -> pure (ActOk (resetVS (a & #stk .~ s')))
    Left  e  -> errAction a e

runViewEffect :: AppState -> CmdInfo -> View AdbcTable -> Effect -> IO Action
runViewEffect a ci v' e = do
  let s  = stk a
      a' = withStk a ci (View.setCur s v')
  case e of
    EffectNone -> pure (ActOk a')
    EffectQuit -> pure ActQuit
    EffectFetchMore -> do
      r <- try (TblOps.fetchMore (View.tbl s))
             :: IO (Either SomeException (Maybe AdbcTable))
      case r of
        Right (Just tbl') ->
          case View.rebuild v' tbl' 0 V.empty (Nav.cur (Nav.row (View.nav v'))) of
            Just rv -> pure (ActOk (resetVS (a' { stk = View.setCur s rv })))
            Nothing -> pure (ActOk a')
        Right Nothing  -> pure (ActOk a')
        Left  err      -> errAction a' err
    EffectSort colIdx sels grp asc -> tryStk a ci $ do
      tbl' <- TblOps.modifyTableSort (View.tbl s) colIdx sels grp asc
      let mrv = View.rebuild v' tbl' colIdx V.empty (Nav.cur (Nav.row (View.nav v')))
      pure (fmap (View.setCur s) mrv)
    EffectExclude cols -> tryStk a ci $ do
      tbl' <- AdbcTable.excludeCols (View.tbl s) cols
      let grp'    = V.filter (not . (`V.elem` cols)) (Nav.grp (View.nav v'))
          hidden' = V.filter (not . (`V.elem` cols)) (Nav.hidden (View.nav v'))
          mrv = View.rebuild v' tbl' 0 grp' (Nav.cur (Nav.row (View.nav v')))
      pure $ fmap (\rv -> View.setCur s (rv & #nav % #hidden .~ hidden')) mrv
    EffectFreq colNames -> tryStk a ci $ do
      mft <- AdbcTable.freqTable (View.tbl s) colNames
      case mft of
        Nothing -> pure Nothing
        Just (adbc, totalGroups) ->
          case View.fromTbl adbc (View.path (View.cur s)) 0 colNames 0 of
            Just fv ->
              pure (Just (View.push s (fv
                { View.vkind = VkFreqV colNames totalGroups
                , View.disp  = "freq " <> TblOps.joinWith colNames ","
                })))
            Nothing -> pure Nothing
    EffectFreqFilter cols row -> tryStk a ci $ do
      case (View.vkind (View.cur s), View.pop s) of
        (VkFreqV _ _, Just s') -> do
          expr <- Freq.filterIO (View.tbl s) cols row
          mf <- TblOps.filter_ (View.tbl s') expr
          case mf of
            Just tbl' ->
              case View.rebuild (View.cur s') tbl' 0 V.empty 0 of
                Just rv -> pure (Just (View.push s' rv))
                Nothing -> pure Nothing
            Nothing -> pure Nothing
        _ -> pure Nothing

viewUp :: AppState -> CmdInfo -> IO Action
viewUp a ci = do
  freqFirst <-
    if ciCmd ci == CmdFreqOpen || ciCmd ci == CmdFreqFilter
      then case Freq.update (stk a) (ciCmd ci) of
             Just (s', e) ->
               Just <$> runViewEffect (withStk a ci s') ci (View.cur s') e
             Nothing -> pure Nothing
      else pure Nothing
  case freqFirst of
    Just r  -> pure r
    Nothing ->
      case View.update (View.cur (stk a)) (ciCmd ci) 20 of
        Just (v', e) -> runViewEffect a ci v' e
        Nothing      -> pure ActUnhandled

-- Handler combinators --

domainH
  :: (ViewStack AdbcTable -> Cmd -> Maybe (IO (Maybe (ViewStack AdbcTable))))
  -> HandlerFn
domainH d = \a ci _ ->
  case d (stk a) (ciCmd ci) of
    Just f  -> tryStk a ci f
    Nothing -> viewUp a ci

domainH'
  :: (ViewStack AdbcTable -> Cmd -> Maybe (IO (ViewStack AdbcTable)))
  -> HandlerFn
domainH' d = \a ci _ ->
  case d (stk a) (ciCmd ci) of
    Just f  -> tryStk a ci (Just <$> f)
    Nothing -> viewUp a ci

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
  pure (case View.updateStack (stk a) (ciCmd ci) of
    Just (_, EffectQuit) -> ActQuit
    Just (s', _)         -> ActOk (withStk a ci s')
    Nothing              -> ActUnhandled)

precSet :: Int -> HandlerFn
precSet v = \a _ _ -> pure (ActOk (set precL v a))

precAdj :: Int -> HandlerFn
precAdj delta = \a _ _ ->
  pure (ActOk (over precL (\p -> min 17 (max 0 (p + delta))) a))
