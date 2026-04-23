module Tv.Theme where

import Tv.Prelude

stylesRef :: IORef (Vector Word32)
getStyles :: IO (Vector Word32)
styleFg :: Vector Word32 -> Int -> Word32
styleBg :: Vector Word32 -> Int -> Word32
sPickerPanel :: Int
sPickerSel   :: Int
sPickerMatch :: Int
