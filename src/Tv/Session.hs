{-
  Session: save/load view stack state to JSON files in ~/.cache/tv/sessions/.
  Serializes Prql.Query pipeline + view metadata; restores by re-executing queries.

  Literal port of Tc/Tc/Session.lean — same function set, same order, same
  comments. Refactor only after parity.
-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Tv.Session where

import Tv.Prelude
import Control.Exception (SomeException, try)
import Data.Aeson (Value(..))
import qualified Data.Aeson as A
import qualified Data.Aeson.Encoding as E
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.Aeson.Types as A
import qualified Data.ByteString.Lazy as BL
import Data.Char (isAlphaNum)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import qualified Data.Vector as V
import System.Directory (createDirectoryIfMissing, listDirectory)
import Tv.Data.DuckDB.Prql (Query(..))
import Tv.Data.DuckDB.Table (AdbcTable)
import qualified Tv.Data.DuckDB.Table as AdbcTable
import qualified Tv.Folder as Folder
import qualified Tv.Fzf as Fzf
import qualified Tv.Nav as Nav
import qualified Tv.Render as Render
import Tv.App.Types (AppState(..), HandlerFn, stackIO)
import Tv.CmdConfig (Entry, mkEntry, hdl)
import Tv.Types
  ( Agg(..)
  , Cmd(..)
  , Op(..)
  , StrEnum(toString)
  , ViewKind(..)
  )
import Tv.View (View(..), ViewStack(..))
import qualified Tv.View as View
import qualified Tv.Log as Log

-- | Session directory under ~/.cache/tv/
sessDir :: IO FilePath
sessDir = do
  d <- Log.dir
  let dir_ = d ++ "/sessions"
  createDirectoryIfMissing True dir_
  pure dir_

-- | Sanitize session name: keep only alphanumeric, dash, underscore, dot
sanitize :: Text -> Text
sanitize name =
  T.pack (filter (\c -> isAlphaNum c || c == '-' || c == '_' || c == '.') (T.unpack name))

-- | Session file path (name is sanitized to prevent path traversal)
sessPath :: Text -> IO FilePath
sessPath name = do
  let safe = sanitize name
  if T.null safe
    then ioError (userError "invalid session name")
    else do
      d <- sessDir
      pure $ d ++ "/" ++ T.unpack safe ++ ".json"

-- ## ToJson / FromJson instances

aggToJ :: Agg -> Value
aggToJ a = String (toString a)

opToJ :: Op -> Value
opToJ op = case op of
  OpFilter e ->
    mkObj [("type", String "filter"), ("expr", String e)]
  OpSort cols ->
    mkObj [("type", String "sort"), ("cols", sortColsJson cols)]
  OpSel cols ->
    mkObj [("type", String "sel"), ("cols", textArrJson cols)]
  OpExclude cols ->
    mkObj [("type", String "exclude"), ("cols", textArrJson cols)]
  OpDerive bs ->
    mkObj [("type", String "derive"), ("bindings", pairArrJson bs)]
  OpGroup keys aggs ->
    let as = V.map
               (\(fn, name, col) ->
                  Array (V.fromList [aggToJ fn, String name, String col]))
               aggs
    in mkObj [("type", String "group"), ("keys", textArrJson keys), ("aggs", Array as)]
  OpTake n ->
    mkObj [("type", String "take"), ("n", Number (fromIntegral n))]
  where
    mkObj :: [(Text, Value)] -> Value
    mkObj ps = Object (KM.fromList (map (\(k, v) -> (K.fromText k, v)) ps))
    textArrJson :: Vector Text -> Value
    textArrJson v = Array (V.map String v)
    sortColsJson :: Vector (Text, Bool) -> Value
    sortColsJson v = Array (V.map (\(c, asc) -> Array (V.fromList [String c, Bool asc])) v)
    pairArrJson :: Vector (Text, Text) -> Value
    pairArrJson v = Array (V.map (\(a, b) -> Array (V.fromList [String a, String b])) v)

vkToJ :: ViewKind -> Value
vkToJ vk = case vk of
  VkTbl ->
    mkObj [("kind", String "tbl")]
  VkFreqV cols total ->
    mkObj [("kind", String "freqV"), ("cols", Array (V.map String cols)), ("total", Number (fromIntegral total))]
  VkColMeta ->
    mkObj [("kind", String "colMeta")]
  VkCorr ->
    mkObj [("kind", String "corr")]
  VkFld path_ depth ->
    mkObj [("kind", String "fld"), ("path", String path_), ("depth", Number (fromIntegral depth))]
  where
    mkObj :: [(Text, Value)] -> Value
    mkObj ps = Object (KM.fromList (map (\(k, v) -> (K.fromText k, v)) ps))

-- ## Serialization: View → JSON

-- Encode a View as a JSON Encoding (ordered, matching Lean byte-for-byte).
viewEncoding :: View AdbcTable -> E.Encoding
viewEncoding v =
  let q = AdbcTable.query (Nav.tbl (View.nav v))
      searchEnc = case search v of
        Just (i, s) ->
          E.pairs
            ( E.pair "col" (E.int i)
           <> E.pair "val" (E.text s) )
        Nothing -> E.null_
      opsEnc = E.list (E.value . opToJ) (V.toList (ops q))
      queryEnc =
        E.pairs
          ( E.pair "base" (E.text (base q))
         <> E.pair "ops" opsEnc )
      n = View.nav v
  in E.pairs
       ( E.pair "path"     (E.text (View.path v))
      <> E.pair "vkind"    (E.value (vkToJ (View.vkind v)))
      <> E.pair "disp"     (E.text (View.disp v))
      <> E.pair "prec"     (E.int (View.prec v))
      <> E.pair "widthAdj" (E.int (View.widthAdj v))
      <> E.pair "row"      (E.int (Nav.cur (Nav.row n)))
      <> E.pair "col"      (E.int (Nav.cur (Nav.col n)))
      <> E.pair "grp"      (E.list E.text (V.toList (Nav.grp n)))
      <> E.pair "hidden"   (E.list E.text (V.toList (Nav.hidden n)))
      <> E.pair "colSels"  (E.list E.text (V.toList (Nav.sels (Nav.col n))))
      <> E.pair "search"   searchEnc
      <> E.pair "query"    queryEnc )

stkToJ :: ViewStack AdbcTable -> Text
stkToJ s =
  let views = View.hd s : View.tl s
      viewsEnc = E.list viewEncoding views
      topEnc = E.pairs
        ( E.pair "version" (E.int 1)
       <> E.pair "views"   viewsEnc )
  in TE.decodeUtf8 (BL.toStrict (E.encodingToLazyByteString topEnc))

-- ## Deserialization: JSON → View state

-- | JSON field with supplied default (Lean's `jd` helper).
jd :: A.FromJSON a => Value -> Text -> a -> a
jd j k deflt = case j of
  Object o -> fromMaybe deflt
    (KM.lookup (K.fromText k) o >>= A.parseMaybe A.parseJSON)
  _        -> deflt

jdMaybe :: A.FromJSON a => Value -> Text -> Maybe a
jdMaybe j k = case j of
  Object o -> KM.lookup (K.fromText k) o >>= A.parseMaybe A.parseJSON
  _        -> Nothing

-- | Get raw sub-Value for a key.
objVal :: Value -> Text -> Maybe Value
objVal (Object o) k = KM.lookup (K.fromText k) o
objVal _ _ = Nothing

-- | Get sub-Value with null fallback (like Lean's `objValD`).
objValD :: Value -> Text -> Value
objValD j k = fromMaybe Null $ objVal j k

-- | Catch IO exceptions, log, return Nothing (matches Lean's per-view skip).
tryIO :: IO (Maybe AdbcTable) -> IO (Maybe AdbcTable)
tryIO act = do
  r <- try act
  case r of
    Right x -> pure x
    Left (e :: SomeException) -> do
      Log.write "session" ("skip view: " <> T.pack (show e))
      pure Nothing

-- | Open the underlying AdbcTable for a view: folder listing or query pipeline.
openView :: Bool -> ViewKind -> Query -> IO (Maybe AdbcTable)
openView noSign_ vkind_ query_ = tryIO $ case vkind_ of
  VkFld p depth -> do
    mv <- Folder.mkView noSign_ p depth
    pure $ fmap (Nav.tbl . View.nav) mv
  _ -> do
    total <- AdbcTable.queryCount query_
    AdbcTable.requery query_ total

-- | Build a View from an opened table and apply saved metadata (cursor, hidden, search…).
applyFields :: AdbcTable -> Value -> Text -> ViewKind -> Maybe (View AdbcTable)
applyFields tbl j path_ vkind_ =
  let nRows_ = AdbcTable.nRows tbl
      nCols_ = V.length (AdbcTable.colNames tbl)
      row_   = jd j "row" 0 :: Int
      col_   = jd j "col" 0 :: Int
      grp_   = jd j "grp" V.empty :: Vector Text
  in if nRows_ == 0 || nCols_ == 0
    then Nothing
    else do
      view <- View.fromTbl tbl path_ (min col_ (nCols_ - 1)) grp_ (min row_ (nRows_ - 1))
      let search_ = do
            s <- objVal j "search"
            case s of
              Null -> Nothing
              _    -> Just (jd s "col" 0 :: Int, jd s "val" "" :: Text)
          nav' = (view ^. #nav)
               & #hidden      .~ (jd j "hidden" V.empty :: Vector Text)
               & #col % #sels .~ (jd j "colSels" V.empty :: Vector Text)
      pure $ view & #vkind    .~ vkind_
                  & #disp     .~ (jd j "disp" "" :: Text)
                  & #prec     .~ fromMaybe 3 (jdMaybe j "prec")
                  & #widthAdj .~ (jd j "widthAdj" 0 :: Int)
                  & #search   .~ search_
                  & #nav      .~ nav'

-- | Restore a single view from JSON, re-executing the query pipeline.
--   Errors are caught per-view so partial restoration works.
restoreView :: Bool -> Value -> IO (Maybe (View AdbcTable))
restoreView noSign_ j = do
  let path_ = jd j "path" "" :: Text
  if T.null path_
    then pure Nothing
    else do
      let vkind_ = fromMaybe VkTbl $ jdMaybe j "vkind"
          qObj   = objValD j "query"
          query_ = Query
            { base = fromMaybe ("from `" <> path_ <> "`") $ jdMaybe qObj "base"
            , ops  = jd qObj "ops" V.empty :: Vector Op
            }
      tblM <- openView noSign_ vkind_ query_
      pure $ tblM >>= \tbl -> applyFields tbl j path_ vkind_

-- ## Public API

-- | Derive session name from view stack (like export uses tabName)
autoName :: ViewStack AdbcTable -> Text
autoName stk =
  let name = T.replace " " "_" (T.replace "/" "_" (View.tabName (View.cur stk)))
      parts = T.splitOn "." name
      stem = case parts of
        (x:_) | not (T.null x) -> x
        _                      -> name
  in sanitize stem

save :: ViewStack AdbcTable -> Text -> IO ()
save stk name = do
  let nm = if T.null name then autoName stk else name
  p <- sessPath nm
  TIO.writeFile p (stkToJ stk)
  let views = View.hd stk : View.tl stk
  Log.write "session"
    ("saved " <> T.pack (show (length views)) <> " view(s) to " <> T.pack p)
  Render.statusMsg ("session saved: " <> nm)

load :: Bool -> Text -> IO (Maybe (ViewStack AdbcTable))
load noSign_ name = do
  p <- sessPath name
  rc <- try (TIO.readFile p) :: IO (Either SomeException Text)
  case rc of
    Left _        -> pure Nothing
    Right content ->
      case A.eitherDecodeStrict (TE.encodeUtf8 content) of
        Left _     -> pure Nothing
        Right json -> do
          let views = jd json "views" V.empty :: Vector Value
          restored <- fmap (V.mapMaybe id) $ V.mapM (restoreView noSign_) views
          if not (V.null restored)
            then pure $ Just $ ViewStack
                               { hd = restored V.! 0
                               , tl = V.toList (V.slice 1 (V.length restored - 1) restored)
                               }
            else pure Nothing

list :: IO (Vector Text)
list = do
  d <- sessDir
  r <- try (listDirectory d) :: IO (Either SomeException [FilePath])
  let entries = case r of
        Left _  -> []
        Right xs -> xs
      stems =
        [ T.pack (take (length e - 5) e)
        | e <- entries, ".json" `isSuffixOf` e ]
  pure $ V.fromList stems
  where
    isSuffixOf s xs = length xs >= length s && drop (length xs - length s) xs == s

-- | Prompt for session name; returns none on cancel or empty input
saveName :: Bool -> IO (Maybe Text)
saveName tm = do
  existing <- list
  let input = T.intercalate "\n" (V.toList existing)
  m <- Fzf.fzf tm (V.fromList ["--prompt=session name: ", "--print-query"]) input
  case m of
    Nothing -> pure Nothing
    Just s  -> do
      let lines_ = filter (not . T.null) (T.splitOn "\n" s)
          pick = case reverse lines_ of
            (x:_) -> x
            []    -> case lines_ of
                       (x:_) -> x
                       []    -> ""
          name = sanitize pick
      pure $ if T.null name then Nothing else Just name

loadName :: Bool -> IO (Maybe Text)
loadName tm = do
  existing <- list
  if V.null existing
    then Render.statusMsg "no saved sessions" >> pure Nothing
    else do
      m <- Fzf.fzf tm (V.fromList ["--prompt=load session: "]) (T.intercalate "\n" (V.toList existing))
      pure $ fmap T.strip m

-- | Save session with explicit name (no fzf). Called by socket/dispatch.
saveWith :: ViewStack AdbcTable -> Text -> IO ()
saveWith stk name =
  if T.null name
    then save stk ""        -- empty name → use auto name
    else save stk (sanitize name)

-- | Load session by name directly (no fzf). Called by socket/dispatch.
loadWith :: Bool -> Text -> IO (Maybe (ViewStack AdbcTable))
loadWith noSign_ name =
  if T.null name then pure Nothing else load noSign_ name

commands :: V.Vector (Entry, Maybe HandlerFn)
commands = V.fromList
  [ hdl (mkEntry CmdSessSave "a" "W" "Save session" False "")
        (\a _ arg -> stackIO a (do saveWith (stk a) arg; pure (stk a)))
  , hdl (mkEntry CmdSessLoad "a" ""  "Load session"  False "")
        (\a _ arg -> stackIO a (do
          ms <- loadWith (noSign a) arg
          case ms of
            Just stk' -> pure stk'
            Nothing   -> pure (stk a)))
  ]

