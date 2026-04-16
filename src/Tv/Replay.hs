{-
  Replay: generate compact PRQL ops string from current view state.
  Displayed on tab line; can be replayed with `tc file -p "ops"`.
-}
{-# LANGUAGE OverloadedStrings #-}
module Tv.Replay
  ( opsStr
  ) where

import Data.Text (Text)

import qualified Tv.Data.ADBC.Prql as Prql
import qualified Tv.Data.ADBC.Table as Tbl
import Tv.View (View(..))
import qualified Tv.Nav as Nav

-- | Extract PRQL pipeline ops from view's query, omitting the `from` clause.
-- Returns empty string if no ops applied.
opsStr :: View Tbl.AdbcTable -> Text
opsStr v = Prql.renderOps (Tbl.query (Nav.tbl (nav v)))
