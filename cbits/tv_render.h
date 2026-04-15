/*
 * tv_render.h — plain-C table rendering shim for Haskell FFI.
 *
 * Port of /home/dh/repo/Tc/c/render.c minus Lean runtime dependencies.
 * All inputs are flat C types; caller owns all memory.
 */
#ifndef TV_RENDER_H
#define TV_RENDER_H

#include <stdint.h>
#include <stddef.h>

#define TVCOL_INTS   0
#define TVCOL_FLOATS 1
#define TVCOL_STRS   2

/* Column payload. Interpret `data` according to `tag`:
 *   TVCOL_INTS   -> const int64_t *data  (length nrows)
 *   TVCOL_FLOATS -> const double  *data  (length nrows; NaN = null)
 *   TVCOL_STRS   -> const char * const *data  (length nrows; empty = null)
 */
typedef struct {
    uint8_t  tag;
    int64_t  nrows;
    const void *data;
} TvCol;

/* Screen cell layout; must match Haskell Storable instance in Tv.Term. */
typedef struct {
    uint32_t ch;
    uint32_t fg;
    uint32_t bg;
} TvCell;

/* Render a full table grid into `cells`. Cells outside the render rectangle
 * (status, tabs, overlays written from Haskell) are left untouched.
 *
 * On return `outWidths[nCols]` holds the base (uncapped) widths per column —
 * caller should cache these for the next call as `inWidths`.
 */
void tv_render_table(
    TvCell *cells, int screenW, int screenH,
    const TvCol *cols,       size_t nCols,
    const char * const *names,
    const char *fmts,        size_t nFmts,
    const uint32_t *inWidths, size_t nInWidths,
    const uint32_t *colIdxs,  size_t nDispCols,
    uint64_t nTotalRows, uint64_t nKeys, uint64_t colOff,
    uint64_t r0, uint64_t r1,
    uint64_t curRow, uint64_t curCol,
    int64_t moveDir,
    const uint32_t *selCols,    size_t nSelCols,
    const uint32_t *selRows,    size_t nSelRows,
    const uint32_t *hiddenCols, size_t nHiddenCols,
    const uint32_t *styles, /* 2*9 = 18 entries (fg,bg per style) */
    int64_t prec, int64_t widthAdj,
    uint8_t heatMode,
    const char * const *sparklines, size_t nSparklines,
    uint32_t *outWidths /* nCols */
);

#endif
