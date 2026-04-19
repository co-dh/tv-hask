/* Pointer-based wrappers around prqlc-c functions that pass CompileResult
   by value. GHC's FFI cannot pass/return structs by value; the SysV ABI uses
   a hidden return pointer for >16-byte structs and splits arguments across
   registers, neither of which Haskell `foreign import` expresses. */
#include "prqlc.h"

void prqlc_compile_ptr(const char *prql, const struct Options *opts,
                       struct CompileResult *out) {
  *out = compile(prql, opts);
}

void prqlc_destroy_ptr(struct CompileResult *res) {
  result_destroy(*res);
}
