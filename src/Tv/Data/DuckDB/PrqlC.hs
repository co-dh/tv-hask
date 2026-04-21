{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}
{-
  FFI binding to libprqlc_c.a — PRQL → SQL compiler (Rust staticlib).
  Replaces the subprocess `prqlc` call. Linked statically; no runtime dep.
-}
module Tv.Data.DuckDB.PrqlC
  ( compileFFI
  ) where

import Tv.Prelude
import Control.Exception (bracket)
import qualified Data.Text as T
import Foreign.C.String (CString, newCString, peekCString)
import Foreign.C.Types (CSize(..), CChar)
import Foreign.Marshal.Alloc (allocaBytes, free)
import Foreign.Ptr (Ptr, nullPtr)
import Foreign.Storable (pokeByteOff, peekByteOff)

-- C `Options { bool format; char *target; bool signature_comment; }`
-- bool is 1 byte, ptr at offset 8 due to alignment, total 24 bytes.
pokeOptions :: Ptr a -> Bool -> Ptr CChar -> Bool -> IO ()
pokeOptions p fmt tgt sig = do
  pokeByteOff p 0  (boolByte fmt)
  pokeByteOff p 8  tgt
  pokeByteOff p 16 (boolByte sig)
  where boolByte b = if b then 1 else 0 :: Word8

-- C `CompileResult { const char *output; const Message *messages; size_t messages_len; }`
-- 24 bytes; we only read `output` and `messages_len`.
foreign import ccall unsafe "prqlc_compile_ptr"
  c_compile :: CString -> Ptr () -> Ptr () -> IO ()

foreign import ccall unsafe "prqlc_destroy_ptr"
  c_result_destroy :: Ptr () -> IO ()

-- | Compile PRQL → SQL via static-linked Rust engine. Just sql on success,
-- Nothing on compile error.
compileFFI :: Text -> Text -> IO (Maybe Text)
compileFFI prql target =
  bracket (newCString (T.unpack prql))   free $ \cPrql   ->
  bracket (newCString (T.unpack target)) free $ \cTarget ->
    allocaBytes 24 $ \optPtr ->
    allocaBytes 24 $ \resPtr -> do
      pokeOptions optPtr True cTarget False
      c_compile cPrql optPtr resPtr
      msgLen <- peekByteOff resPtr 16 :: IO CSize
      out    <- peekByteOff resPtr 0  :: IO (Ptr CChar)
      result <- if msgLen == 0 && out /= nullPtr
                  then Just . T.pack <$> peekCString out
                  else pure Nothing
      c_result_destroy resPtr
      pure result
