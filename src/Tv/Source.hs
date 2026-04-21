{-# LANGUAGE ScopedTypeVariables #-}
{-
  Source: central interface for remote/virtual directory sources.

  Each backend lives in its own module under Tv.Source.* and exports a
  `Source` value whose closures hold all the backend-specific logic
  (shell cmds, SQL, URL templates, etc.). This module only does prefix
  lookup and thin runner wrappers — no pattern matching on modes.
-}
module Tv.Source
  ( Source (..)
  , OpenResult (..)
  , sources
  , findSource
  , runList
  , runOpen
  , configParent
  ) where

import Tv.Prelude
import qualified Data.Text as T
import qualified Data.Vector as V

import Tv.Data.DuckDB.Table (AdbcTable)
import Tv.Source.Core (Source (..), OpenResult (..))
import qualified Tv.Source.Core as Core
import qualified Tv.Source.S3 as S3
import qualified Tv.Source.Ftp as Ftp
import qualified Tv.Source.HfRoot as HfRoot
import qualified Tv.Source.HfDataset as HfDataset
import qualified Tv.Source.Rest as Rest
import qualified Tv.Source.Osquery as Osquery
import qualified Tv.Source.Pg as Pg

-- | All source backends. Order mirrors original SourceConfig.sources; the
-- resolver uses longest-prefix match (see `findSource`), so order only
-- breaks ties where prefixes are equal length.
sources :: Vector Source
sources = V.fromList
  [ S3.s3
  , HfDataset.hfDataset
  , HfRoot.hfRoot
  , Rest.rest
  , Osquery.osquery
  , Ftp.ftp
  , Pg.pg
  ]

-- | Longest-prefix match. Used by every callsite that needs to know which
-- backend (if any) handles a path.
findSource :: Text -> Maybe Source
findSource path_ = V.foldl' step Nothing sources
  where
    step best src =
      if not (T.null (pfx src)) && T.isPrefixOf (pfx src) path_
        then case best of
               Just b
                 | T.length (pfx src) > T.length (pfx b) -> Just src
                 | otherwise                              -> best
               Nothing -> Just src
        else best

runList :: Bool -> Source -> Text -> IO (Maybe AdbcTable)
runList n src p = Core.withCache p (list src n p)

runOpen :: Bool -> Source -> Text -> IO OpenResult
runOpen n src p = open src n p

configParent :: Source -> Text -> Maybe Text
configParent = parent
