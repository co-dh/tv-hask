{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-orphans #-}  -- ToJSON/FromJSON for Agg/Op/ViewKind live here; Types is off-limits
-- | Session: save/restore view state to JSON files under ~/.cache/tv/sessions/.
--
-- Port of Tc/Session.lean. A 'View' holds a 'TblOps' record-of-functions which
-- cannot be serialized, so we persist only the fields needed to re-open the
-- source and replay the pipeline: path, vkind, disp, prec/width adjustments,
-- cursor position, group/hidden columns, selections, search, and the Op
-- pipeline. 'loadSession' returns these saved views ('SavedView'); re-executing
-- them into live 'View's is the caller's job (App wiring, TODO when App owner
-- adds the hook). This mirrors Tc.Session.save/load on the data that round-trips.
module Tv.Session
  ( SavedView (..)
  , SavedSession (..)
  , sanitize
  , autoName
  , sessDir
  , sessPath
  , toSaved
  , stackToSaved
  , encodeSession
  , decodeSession
  , saveSession
  , loadSession
  , listSessions
  ) where

import Control.Exception (SomeException, try)
import Data.Aeson ((.:), (.:?), (.=))
import qualified Data.Aeson as A
import qualified Data.Aeson.Types as A
import qualified Data.ByteString.Lazy as BL
import Data.Char (isAlphaNum)
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V
import System.Directory
  ( createDirectoryIfMissing, doesDirectoryExist, doesFileExist, listDirectory )
import System.FilePath ((</>), takeExtension, dropExtension)

import Tv.Types
import Tv.Util (logDir, logWrite)
import Tv.View
import Optics.Core ((^.), (%), (&), (.~), (%~))

-- ============================================================================
-- SavedView / SavedSession: the serializable projection of a ViewStack
-- ============================================================================

-- | Everything needed to re-open a view: source path, vkind, display options,
-- cursor state, group/hidden columns, selections, search, and the Op pipeline
-- (base query + ops) as typed values.
data SavedView = SavedView
  { svPath     :: !Text
  , svVkind    :: !ViewKind
  , svDisp     :: !Text
  , svPrecAdj  :: !Int
  , svWidthAdj :: !Int
  , svRow      :: !Int
  , svCol      :: !Int
  , svGrp      :: !(Vector Text)
  , svHidden   :: !(Vector Text)
  , svColSels  :: !(Vector Text)        -- selected column names (name-based: path-independent)
  , svSearch   :: !(Maybe (Int, Text))  -- last search (colIdx, value)
  , svQueryBase :: !Text
  , svQueryOps  :: !(Vector Op)
  } deriving (Eq, Show)

data SavedSession = SavedSession
  { ssVersion :: !Int
  , ssViews   :: ![SavedView]
  } deriving (Eq, Show)

-- ============================================================================
-- Path / name helpers
-- ============================================================================

-- | Keep only alnum, dash, underscore, dot. Defends against path traversal.
sanitize :: Text -> Text
sanitize = T.filter (\c -> isAlphaNum c || c == '-' || c == '_' || c == '.')

-- | ~/.cache/tv/sessions/, created if missing.
sessDir :: IO FilePath
sessDir = do
  d <- (</> "sessions") <$> logDir
  createDirectoryIfMissing True d
  pure d

-- | Full path for session name (sanitized). Returns Nothing if name empties out.
sessPath :: Text -> IO (Maybe FilePath)
sessPath name =
  let safe = sanitize name
  in if T.null safe
       then pure Nothing
       else do d <- sessDir; pure (Just (d </> T.unpack safe <> ".json"))

-- | Derive session name from the current view's tab name (mirrors Tc.Session.autoName).
-- Slashes and spaces → underscores; drop first dot-extension; sanitize.
autoName :: ViewStack -> Text
autoName vs =
  let v    = vs ^. vsHd
      raw  = case v ^. vNav % nsVkind of
               VFld p _ | T.null (v ^. vDisp) || "/" `T.isPrefixOf` p -> p
               _ | T.null (v ^. vDisp) ->
                     let parts = T.splitOn "/" (v ^. vPath)
                     in if null parts then v ^. vPath else last parts
                 | otherwise -> v ^. vDisp
      name = T.replace " " "_" (T.replace "/" "_" raw)
      stem = case filter (not . T.null) (T.splitOn "." name) of
               (s:_) -> s
               []    -> name
  in sanitize stem

-- ============================================================================
-- View → SavedView
-- ============================================================================

toSaved :: View -> SavedView
toSaved v =
  let ns = v ^. vNav
  in SavedView
       { svPath      = v ^. vPath
       , svVkind     = ns ^. nsVkind
       , svDisp      = v ^. vDisp
       , svPrecAdj   = ns ^. nsPrecAdj
       , svWidthAdj  = ns ^. nsWidthAdj
       , svRow       = ns ^. nsRow % naCur
       , svCol       = ns ^. nsCol % naCur
       , svGrp       = ns ^. nsGrp
       , svHidden    = ns ^. nsHidden
       , svColSels   = colSelNames ns
       , svSearch    = v ^. vSearch
       , svQueryBase = "from `" <> v ^. vPath <> "`"  -- Prql.Query not yet threaded; App builds at load
       , svQueryOps  = V.empty                        -- pipeline lives in App; stub for now
       }
  where
    -- Translate col axis sels (indices into dispIdxs) to concrete column names.
    colSelNames ns =
      let names = ns ^. nsTbl % tblColNames
          disp  = ns ^. nsDispIdxs
          sels  = ns ^. nsCol % naSels
          toName i = if i >= 0 && i < V.length disp
                       then let j = disp V.! i
                            in if j >= 0 && j < V.length names then Just (names V.! j) else Nothing
                       else Nothing
      in V.mapMaybe toName sels

stackToSaved :: ViewStack -> SavedSession
stackToSaved (ViewStack hd tl) =
  SavedSession { ssVersion = 1, ssViews = map toSaved (hd : tl) }

-- ============================================================================
-- Aeson instances
-- ============================================================================

instance A.ToJSON Agg where
  toJSON = A.String . aggStr

instance A.FromJSON Agg where
  parseJSON = A.withText "Agg" $ \t -> case t of
    "count" -> pure ACount; "sum" -> pure ASum; "avg" -> pure AAvg
    "min"   -> pure AMin;   "max" -> pure AMax; "stddev" -> pure AStddev
    "dist"  -> pure ADist
    _ -> fail ("unknown agg: " <> T.unpack t)

instance A.ToJSON Op where
  toJSON = \case
    OpFilter e    -> A.object ["type" .= ("filter" :: Text), "expr" .= e]
    OpSort cols   -> A.object ["type" .= ("sort" :: Text), "cols" .= cols]
    OpSel cols    -> A.object ["type" .= ("sel" :: Text), "cols" .= cols]
    OpExclude cs  -> A.object ["type" .= ("exclude" :: Text), "cols" .= cs]
    OpDerive bs   -> A.object ["type" .= ("derive" :: Text), "bindings" .= bs]
    OpGroup ks as -> A.object ["type" .= ("group" :: Text), "keys" .= ks, "aggs" .= as]
    OpTake n      -> A.object ["type" .= ("take" :: Text), "n" .= n]

instance A.FromJSON Op where
  parseJSON = A.withObject "Op" $ \o -> do
    ty <- o .: "type"
    case (ty :: Text) of
      "filter"  -> OpFilter <$> o .: "expr"
      "sort"    -> OpSort <$> o .: "cols"
      "sel"     -> OpSel <$> o .: "cols"
      "exclude" -> OpExclude <$> o .: "cols"
      "derive"  -> OpDerive <$> o .: "bindings"
      "group"   -> OpGroup <$> o .: "keys" <*> o .: "aggs"
      "take"    -> OpTake <$> o .: "n"
      _ -> fail ("unknown op type: " <> T.unpack ty)

instance A.ToJSON ViewKind where
  toJSON = \case
    VTbl         -> A.object ["kind" .= ("tbl" :: Text)]
    VFreq cs tot -> A.object ["kind" .= ("freqV" :: Text), "cols" .= cs, "total" .= tot]
    VColMeta     -> A.object ["kind" .= ("colMeta" :: Text)]
    VFld p d     -> A.object ["kind" .= ("fld" :: Text), "path" .= p, "depth" .= d]

instance A.FromJSON ViewKind where
  parseJSON = A.withObject "ViewKind" $ \o -> do
    kind <- o .: "kind"
    case (kind :: Text) of
      "tbl"     -> pure VTbl
      "freqV"   -> VFreq <$> o .: "cols" <*> o .: "total"
      "colMeta" -> pure VColMeta
      "fld"     -> VFld <$> o .: "path" <*> o .: "depth"
      _         -> pure VTbl

instance A.ToJSON SavedView where
  toJSON s = A.object
    [ "path"     .= svPath s, "vkind"   .= svVkind s
    , "disp"     .= svDisp s, "prec"    .= svPrecAdj s, "widthAdj" .= svWidthAdj s
    , "row"      .= svRow s,  "col"     .= svCol s
    , "grp"      .= svGrp s,  "hidden"  .= svHidden s, "colSels" .= svColSels s
    , "search"   .= searchJson (svSearch s)
    , "query"    .= A.object ["base" .= svQueryBase s, "ops" .= svQueryOps s]
    ]
    where
      searchJson Nothing        = A.Null
      searchJson (Just (i, v))  = A.object ["col" .= i, "val" .= v]

instance A.FromJSON SavedView where
  parseJSON = A.withObject "SavedView" $ \o -> do
    path     <- o .:  "path"
    vkind    <- fromMaybe VTbl <$> o .:? "vkind"
    disp     <- fromMaybe ""   <$> o .:? "disp"
    precAdj  <- fromMaybe 0    <$> o .:? "prec"
    widthAdj <- fromMaybe 0    <$> o .:? "widthAdj"
    row      <- fromMaybe 0    <$> o .:? "row"
    col      <- fromMaybe 0    <$> o .:? "col"
    grp      <- fromMaybe V.empty <$> o .:? "grp"
    hidden   <- fromMaybe V.empty <$> o .:? "hidden"
    colSels  <- fromMaybe V.empty <$> o .:? "colSels"
    search   <- parseSearch =<< (o .:? "search")
    (qBase, qOps) <- do
      mq <- o .:? "query"
      case mq of
        Nothing -> pure ("from `" <> path <> "`", V.empty)
        Just q  -> A.withObject "query" (\qo -> do
                     b <- fromMaybe ("from `" <> path <> "`") <$> qo .:? "base"
                     ops <- fromMaybe V.empty <$> qo .:? "ops"
                     pure (b, ops)) q
    pure SavedView
      { svPath = path, svVkind = vkind, svDisp = disp
      , svPrecAdj = precAdj, svWidthAdj = widthAdj
      , svRow = row, svCol = col, svGrp = grp, svHidden = hidden
      , svColSels = colSels, svSearch = search
      , svQueryBase = qBase, svQueryOps = qOps
      }
    where
      parseSearch :: Maybe A.Value -> A.Parser (Maybe (Int, Text))
      parseSearch Nothing        = pure Nothing
      parseSearch (Just A.Null)  = pure Nothing
      parseSearch (Just v)       = A.withObject "search"
        (\o -> do i <- o .: "col"; s <- o .: "val"; pure (Just (i, s))) v

instance A.ToJSON SavedSession where
  toJSON s = A.object ["version" .= ssVersion s, "views" .= ssViews s]

instance A.FromJSON SavedSession where
  parseJSON = A.withObject "SavedSession" $ \o -> do
    ver <- fromMaybe 1 <$> o .:? "version"
    vs  <- fromMaybe [] <$> o .:? "views"
    pure (SavedSession ver vs)

-- ============================================================================
-- Encode / decode / save / load
-- ============================================================================

encodeSession :: SavedSession -> BL.ByteString
encodeSession = A.encode

decodeSession :: BL.ByteString -> Maybe SavedSession
decodeSession = A.decode

-- | Persist a ViewStack under a name (sanitized). Empty name → autoName.
-- On success the session file path is written to the log.
saveSession :: Text -> ViewStack -> IO (Maybe FilePath)
saveSession nameIn vs = do
  let name = if T.null (sanitize nameIn) then autoName vs else nameIn
  mpath <- sessPath name
  case mpath of
    Nothing -> do
      logWrite "session" ("save: invalid name " <> T.unpack nameIn)
      pure Nothing
    Just p -> do
      r <- try (BL.writeFile p (encodeSession (stackToSaved vs))) :: IO (Either SomeException ())
      case r of
        Left e  -> logWrite "session" ("save failed: " <> show e) >> pure Nothing
        Right _ -> do
          logWrite "session" ("saved " <> show (length (ssViews (stackToSaved vs))) <> " view(s) to " <> p)
          pure (Just p)

-- | Load a saved session by name. Returns Nothing on missing file or parse error.
-- NOTE: returns 'SavedSession', not a live 'ViewStack' — the TblOps pipeline
-- has to be re-executed by App/SourceConfig before a View can exist.
loadSession :: Text -> IO (Maybe SavedSession)
loadSession name = sessPath name >>= \case
  Nothing -> pure Nothing
  Just p  -> (try (BL.readFile p) :: IO (Either SomeException BL.ByteString)) >>= \case
    Left _   -> pure Nothing
    Right bs -> case decodeSession bs of
      Nothing -> Nothing <$ logWrite "session" ("parse failed: " <> p)
      Just s  -> pure (Just s)

-- | List available session names (*.json stems) in ~/.cache/tv/sessions/.
listSessions :: IO [Text]
listSessions = do
  d <- sessDir
  exists <- doesDirectoryExist d
  if not exists then pure [] else
    either (const []) (mapMaybe stem) <$>
      (try (listDirectory d) :: IO (Either SomeException [FilePath]))
  where
    stem f | takeExtension f == ".json" = Just (T.pack (dropExtension f))
           | otherwise                  = Nothing
