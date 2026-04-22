-- Facade: aggregates per-feature command tables. Lets App.Common
-- drop the ~7 single-use feature-module imports that exist only
-- to call X.commands once.
module Tv.App.FeatureCommands (featureCommands) where

import Tv.Prelude
import Tv.App.Types (HandlerFn)
import Tv.CmdConfig (Entry)
import qualified Tv.Diff as Diff
import qualified Tv.Eda as Eda
import qualified Tv.Export as Export
import qualified Tv.Filter as Filter
import qualified Tv.Folder as Folder
import qualified Tv.Meta as Meta
import qualified Tv.Nav as Nav
import qualified Tv.Ops as Ops
import qualified Tv.Plot as Plot
import qualified Tv.Session as Session
import qualified Tv.Transpose as Transpose

featureCommands :: Vector (Entry, Maybe HandlerFn)
featureCommands = Nav.commands <> Filter.commands <> Ops.commands
  <> Plot.commands <> Meta.commands <> Folder.commands
  <> Export.commands <> Session.commands
  <> Transpose.commands <> Diff.commands <> Eda.commands
