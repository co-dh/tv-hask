/*
 * tv_render.c — plain-C table render shim.
 *
 * Literal port of /home/dh/repo/Tc/c/render.c with Lean runtime access
 * replaced by plain C struct/array access. Function body structure and
 * comments are preserved so future audits can diff the two side-by-side.
 * Heat mode is ported inline from /home/dh/repo/Tc/c/heat.c.
 */
#include "tv_render.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

int tv_term_size(int *rows, int *cols) {
    struct winsize ws;
    if (ioctl(STDIN_FILENO, TIOCGWINSZ, &ws) < 0) return -1;
    if (ws.ws_row == 0 || ws.ws_col == 0) return -1;
    *rows = ws.ws_row;
    *cols = ws.ws_col;
    return 0;
}

/* === Style indices (shared with Lean reference) === */
#define STYLE_CURSOR     0
#define STYLE_SEL_ROW    1
#define STYLE_SEL_CUR    2
#define STYLE_SEL_COL    3
#define STYLE_CUR_ROW    4
#define STYLE_CUR_COL    5
#define STYLE_DEFAULT    6
#define STYLE_HEADER     7
#define STYLE_GROUP      8
#define NUM_STYLES       9
#define SEP_FG           240u  /* 256-colour medium gray separator */

#define TB_BOLD      0x01000000u
#define TB_UNDERLINE 0x02000000u

#define MIN_HDR_WIDTH  3
#define MAX_DISP_WIDTH 50
#define MAX_PREC       17

/* ------------------------------------------------------------------ */
/*  UTF-8 helpers (inlined from Tc/c/term_core.c)                     */
/* ------------------------------------------------------------------ */

static uint32_t utf8_decode(const char **pp) {
    const unsigned char *p = (const unsigned char *)*pp;
    uint32_t ch;
    if (p[0] < 0x80) { ch = p[0]; *pp += 1; }
    else if ((p[0] & 0xE0) == 0xC0) { ch = ((p[0] & 0x1F) << 6) | (p[1] & 0x3F); *pp += 2; }
    else if ((p[0] & 0xF0) == 0xE0) { ch = ((p[0] & 0x0F) << 12) | ((p[1] & 0x3F) << 6) | (p[2] & 0x3F); *pp += 3; }
    else if ((p[0] & 0xF8) == 0xF0) { ch = ((p[0] & 0x07) << 18) | ((p[1] & 0x3F) << 12) | ((p[2] & 0x3F) << 6) | (p[3] & 0x3F); *pp += 4; }
    else { ch = '?'; *pp += 1; }
    return ch;
}

static size_t utf8_len(const char *s, size_t max) {
    size_t len = 0;
    while (*s && len < max) { utf8_decode(&s); len++; }
    return len;
}

/* ------------------------------------------------------------------ */
/*  Cell buffer writer (replaces hd_set_cell / tb_set_cell)           */
/* ------------------------------------------------------------------ */

/* Render state used by the static helpers below. */
typedef struct {
    TvCell *cells;
    int w;
    int h;
} Ctx;

static inline void set_cell(Ctx *ctx, int x, int y, uint32_t ch, uint32_t fg, uint32_t bg) {
    if (x < 0 || y < 0 || x >= ctx->w || y >= ctx->h) return;
    TvCell *c = &ctx->cells[y * ctx->w + x];
    c->ch = ch;
    c->fg = fg;
    c->bg = bg;
}

/* ------------------------------------------------------------------ */
/*  Value formatting helpers                                          */
/* ------------------------------------------------------------------ */

/* VisiData-style type chars: # int, % float, ? bool, @ date, space string */
static char type_char_fmt(char fmt) {
    switch (fmt) {
    case 'l': case 'i': case 's': case 'c':
    case 'L': case 'I': case 'S': case 'C':
        return '#';
    case 'g': case 'f': case 'e':
        return '%';
    case 'b':
        return '?';
    case 't':
        return '@';
    default:
        return ' ';
    }
}

static char type_char_col(const TvCol *col) {
    switch (col->tag) {
    case TVCOL_INTS:   return '#';
    case TVCOL_FLOATS: return '%';
    default:           return ' ';
    }
}

static int fmt_int_comma(char *buf, size_t buflen, int64_t v) {
    char tmp[32];
    int neg = v < 0;
    uint64_t u = neg ? (uint64_t)(-(v + 1)) + 1 : (uint64_t)v;
    int i = 0;
    do { tmp[i++] = '0' + (u % 10); u /= 10; } while (u);
    int len = i + (i - 1) / 3 + neg;
    if ((size_t)len >= buflen) return snprintf(buf, buflen, "%lld", (long long)v);
    char *p = buf;
    if (neg) *p++ = '-';
    int g = (i - 1) % 3 + 1;
    while (i > 0) {
        *p++ = tmp[--i];
        if (--g == 0 && i > 0) { *p++ = ','; g = 3; }
    }
    *p = '\0';
    return len;
}

static int format_col_cell(const TvCol *col, size_t row, char *buf, size_t buflen, int prec) {
    if (row >= (size_t)col->nrows) { buf[0] = '\0'; return 0; }
    switch (col->tag) {
    case TVCOL_INTS: {
        const int64_t *d = (const int64_t *)col->data;
        return fmt_int_comma(buf, buflen, d[row]);
    }
    case TVCOL_FLOATS: {
        const double *d = (const double *)col->data;
        double f = d[row];
        if (f != f) { buf[0] = '\0'; return 0; }
        if (prec < 0) prec = 0;
        if (prec > MAX_PREC) prec = MAX_PREC;
        return snprintf(buf, buflen, "%.*f", prec, f);
    }
    case TVCOL_STRS: {
        const char * const *d = (const char * const *)col->data;
        const char *s = d[row];
        if (!s || !s[0]) { buf[0] = '\0'; return 0; }
        size_t len = strlen(s);
        if (len >= buflen) len = buflen - 1;
        memcpy(buf, s, len);
        buf[len] = '\0';
        return (int)len;
    }
    default:
        buf[0] = '\0';
        return 0;
    }
}

static int col_is_num(const TvCol *col) {
    return col->tag == TVCOL_INTS || col->tag == TVCOL_FLOATS;
}

static int compute_data_width(const TvCol *col, size_t r0, size_t r1, int prec) {
    int w = 1;
    char buf[256];
    for (size_t r = r0; r < r1; r++) {
        int len = format_col_cell(col, r, buf, sizeof(buf), prec);
        if (len > w) w = len;
    }
    return w;
}

static void print_pad(Ctx *ctx, int x, int y, int w, uint32_t fg, uint32_t bg, const char *s, int right) {
    if (!s) s = "";
    int len = (int)utf8_len(s, (size_t)w);
    if (len > w) len = w;
    int pad = w - len;
    int cx = x;
    if (right) for (int i = 0; i < pad; i++) set_cell(ctx, cx++, y, ' ', fg, bg);
    const char *p = s;
    for (int i = 0; i < len && *p; i++) {
        uint32_t ch = utf8_decode(&p);
        set_cell(ctx, cx++, y, ch, fg, bg);
    }
    if (!right) for (int i = 0; i < pad; i++) set_cell(ctx, cx++, y, ' ', fg, bg);
}

static int get_style(int isCursor, int isSelRow, int isSel, int isCurRow, int isCurCol) {
    if (isCursor)          return STYLE_CURSOR;
    if (isSelRow)          return STYLE_SEL_ROW;
    if (isSel && isCurRow) return STYLE_SEL_CUR;
    if (isSel)             return STYLE_SEL_COL;
    if (isCurRow)          return STYLE_CUR_ROW;
    if (isCurCol)          return STYLE_CUR_COL;
    return STYLE_DEFAULT;
}

static void build_sel_bits(const uint32_t *arr, size_t n, uint64_t *bits) {
    bits[0] = bits[1] = bits[2] = bits[3] = 0;
    for (size_t i = 0; i < n; i++) {
        uint32_t v = arr[i];
        if (v < 256) bits[v / 64] |= 1ULL << (v % 64);
    }
}
#define IS_SEL(bits, v) ((v) < 256 && ((bits)[(v)/64] & (1ULL << ((v)%64))))

/* ------------------------------------------------------------------ */
/*  Heat mode — inlined port of Tc/c/heat.c                           */
/* ------------------------------------------------------------------ */

#define MAX_HEAT_COLS 256
#define HEAT_FG       16u   /* black text on coloured bg */
#define HEAT_NONE     0
#define HEAT_NUM      1
#define HEAT_STR      2

typedef struct {
    double mn, mx;
    int    kind;
    char   date;
} HeatCol;

static int heat_col_num_val(const TvCol *col, size_t row, double *out) {
    if (row >= (size_t)col->nrows) return 0;
    if (col->tag == TVCOL_INTS) {
        const int64_t *d = (const int64_t *)col->data;
        *out = (double)d[row];
        return 1;
    } else if (col->tag == TVCOL_FLOATS) {
        const double *d = (const double *)col->data;
        double v = d[row];
        if (isnan(v)) return 0;
        *out = v;
        return 1;
    }
    return 0;
}

/* FNV-1a hash → [0, 1] for categorical string coloring. */
static double heat_str_hash01(const char *s) {
    uint32_t h = 2166136261u;
    while (*s) { h ^= (unsigned char)*s++; h *= 16777619u; }
    return (double)(h & 0xFFFF) / 65535.0;
}

/* Extract digits from date/time string → monotonic double. */
static double heat_date_to_num(const char *s) {
    double v = 0;
    while (*s) {
        if (*s >= '0' && *s <= '9') v = v * 10 + (*s - '0');
        s++;
    }
    return v;
}

static int heat_is_date_fmt(char fmt) { return fmt == 't'; }

static uint32_t heat_color(double t) {
    /* Viridis-inspired purple→teal→green→yellow ramp via xterm-256 cube. */
    static const uint32_t ramp[] = {
        53, 54, 55,
        61, 25, 31,
        30, 36, 42,
        41, 77, 113,
        149, 148, 184,
        190, 226,
    };
    static const int N = (int)(sizeof(ramp) / sizeof(ramp[0]));
    if (t <= 0.0) return ramp[0];
    if (t >= 1.0) return ramp[N - 1];
    double pos = t * (N - 1);
    int lo = (int)pos;
    if (lo >= N - 1) lo = N - 2;
    return (pos - lo < 0.5) ? ramp[lo] : ramp[lo + 1];
}

static void heat_scan(const TvCol *cols, const uint32_t *colIdxs,
                      const size_t *dispIdxs, size_t nVisCols,
                      size_t nRows, uint64_t r0,
                      const char *fmts, size_t nFmts,
                      HeatCol *out) {
    for (size_t c = 0; c < nVisCols && c < MAX_HEAT_COLS; c++) {
        size_t origIdx = (size_t)colIdxs[dispIdxs[c]];
        const TvCol *col = &cols[origIdx];
        out[c].kind = HEAT_NONE;
        out[c].date = 0;
        char fmt = (fmts && origIdx < nFmts) ? fmts[origIdx] : 0;
        if (col->tag == TVCOL_INTS || col->tag == TVCOL_FLOATS) {
            double mn = 1e308, mx = -1e308;
            for (size_t ri = 0; ri < nRows; ri++) {
                double v;
                if (!heat_col_num_val(col, (size_t)(r0 + ri), &v)) continue;
                if (v < mn) mn = v;
                if (v > mx) mx = v;
            }
            if (mx > mn) { out[c].mn = mn; out[c].mx = mx; out[c].kind = HEAT_NUM; }
        } else if (col->tag == TVCOL_STRS && heat_is_date_fmt(fmt)) {
            const char * const *d = (const char * const *)col->data;
            double mn = 1e308, mx = -1e308;
            for (size_t ri = 0; ri < nRows; ri++) {
                const char *s = d[r0 + ri];
                if (!s || !s[0]) continue;
                double v = heat_date_to_num(s);
                if (v < mn) mn = v;
                if (v > mx) mx = v;
            }
            if (mx > mn) { out[c].mn = mn; out[c].mx = mx; out[c].kind = HEAT_NUM; out[c].date = 1; }
        } else if (col->tag == TVCOL_STRS) {
            const char * const *d = (const char * const *)col->data;
            const char *first = NULL;
            int diverse = 0;
            for (size_t ri = 0; ri < nRows && !diverse; ri++) {
                const char *s = d[r0 + ri];
                if (!s) s = "";
                if (!first) first = s;
                else if (strcmp(first, s) != 0) diverse = 1;
            }
            if (diverse) out[c].kind = HEAT_STR;
        }
    }
}

static int heat_cell_bg(const TvCol *col, uint64_t row, size_t c, int si,
                        uint8_t mode, const HeatCol *cols, uint32_t *bg) {
    if (c >= MAX_HEAT_COLS || cols[c].kind == HEAT_NONE) return 0;
    if (si == STYLE_CURSOR || si == STYLE_SEL_ROW || si == STYLE_SEL_CUR) return 0;
    /* mode: 1=numeric (HEAT_NUM), 2=categorical (HEAT_STR), 3=both */
    if (cols[c].kind == HEAT_NUM && !(mode & 1)) return 0;
    if (cols[c].kind == HEAT_STR && !(mode & 2)) return 0;
    double t;
    if (cols[c].kind == HEAT_NUM) {
        double v;
        if (cols[c].date) {
            const char * const *d = (const char * const *)col->data;
            const char *s = d[row];
            if (!s || !s[0]) return 0;
            v = heat_date_to_num(s);
        } else {
            if (!heat_col_num_val(col, (size_t)row, &v)) return 0;
        }
        t = (v - cols[c].mn) / (cols[c].mx - cols[c].mn);
    } else if (cols[c].kind == HEAT_STR) {
        const char * const *d = (const char * const *)col->data;
        const char *s = d[row];
        if (!s || !s[0]) return 0;
        t = heat_str_hash01(s);
    } else {
        return 0;
    }
    *bg = heat_color(t);
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Main entry point                                                  */
/* ------------------------------------------------------------------ */

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
    const uint32_t *styles,
    int64_t prec, int64_t widthAdj,
    uint8_t heatMode,
    const char * const *sparklines, size_t nSparklines,
    uint32_t *outWidths)
{
    (void)nTotalRows;

    Ctx ctx = { cells, screenW, screenH };
    size_t nRows = (r1 > r0) ? (size_t)(r1 - r0) : 0;
    char buf[256];

    /* sparkline: active if array is non-empty and has at least one non-empty string */
    int sparkOn = 0;
    for (size_t i = 0; i < nSparklines && !sparkOn; i++) {
        const char *sp = sparklines[i];
        if (sp && sp[0]) sparkOn = 1;
    }

    /* extract styles */
    uint32_t stFg[NUM_STYLES], stBg[NUM_STYLES];
    for (int s = 0; s < NUM_STYLES; s++) {
        stFg[s] = styles[s * 2];
        stBg[s] = styles[s * 2 + 1];
    }

    /* build selection bitsets */
    uint64_t colBits[4], rowBits[4], hidBits[4];
    build_sel_bits(selCols,    nSelCols,    colBits);
    build_sel_bits(selRows,    nSelRows,    rowBits);
    build_sel_bits(hiddenCols, nHiddenCols, hidBits);

    /* Compute base / rendered widths for ALL columns. Matches render.c
     * exactly: scan only visible rows, honour cached widths monotonically,
     * apply MAX_DISP_WIDTH cap + widthAdj separately. */
    int *baseWidths = (int *)malloc(nCols * sizeof(int));
    int *allWidths  = (int *)malloc(nCols * sizeof(int));
    for (size_t c = 0; c < nCols; c++) {
        if (IS_SEL(hidBits, c)) { baseWidths[c] = 0; allWidths[c] = 3; continue; }
        int cached = (c < nInWidths) ? (int)inWidths[c] : 0;
        int dw = compute_data_width(&cols[c], (size_t)r0, (size_t)r1, (int)prec);
        int base = (dw > MIN_HDR_WIDTH ? dw : MIN_HDR_WIDTH) + 2;
        if (cached > base) base = cached;
        baseWidths[c] = base;
        int disp = base > MAX_DISP_WIDTH ? MAX_DISP_WIDTH : base;
        int w = disp + (int)widthAdj;
        if (w < 3) w = 3;
        allWidths[c] = w;
    }

    /* compute x positions: key columns pinned left, then scrollable */
    size_t *dispIdxsArr = (size_t *)malloc(nDispCols * sizeof(size_t));
    int *xs = (int *)malloc(nDispCols * sizeof(int));
    int *ws = (int *)malloc(nDispCols * sizeof(int));
    size_t nVisCols = 0;

    /* pinned key width */
    int keyWidth = 0;
    for (size_t c = 0; c < nKeys && c < nDispCols; c++) {
        size_t origIdx = (size_t)colIdxs[c];
        keyWidth += allWidths[origIdx] + 1;
    }

    /* find cursor's display index relative to non-key columns */
    size_t curDispIdx = 0;
    for (size_t c = nKeys; c < nDispCols; c++) {
        size_t origIdx = (size_t)colIdxs[c];
        if (origIdx == curCol) { curDispIdx = c - nKeys; break; }
    }

    /* adjust colOff so cursor column is visible */
    int scrollW = screenW - keyWidth;
    if (scrollW < 1) scrollW = 1;
    if (curDispIdx < colOff) colOff = curDispIdx;
    for (;;) {
        int cumX = 0;
        for (size_t c = colOff; c <= curDispIdx && (nKeys + c) < nDispCols; c++) {
            size_t origIdx = (size_t)colIdxs[nKeys + c];
            cumX += allWidths[origIdx] + 1;
        }
        if (cumX <= scrollW || colOff >= curDispIdx) break;
        colOff++;
    }

    /* 1. render pinned key columns */
    int x = 0;
    for (size_t c = 0; c < nKeys && c < nDispCols && x < screenW; c++) {
        size_t origIdx = (size_t)colIdxs[c];
        int w = allWidths[origIdx];
        if (x + w > screenW) w = screenW - x;
        dispIdxsArr[nVisCols] = c;
        xs[nVisCols] = x;
        ws[nVisCols] = w;
        nVisCols++;
        x += w + 1;
    }
    size_t visKeys = nVisCols;

    /* 2. render non-key columns from colOff onward */
    size_t nonKeyStart = nKeys + colOff;
    for (size_t c = nonKeyStart; c < nDispCols && x < screenW; c++) {
        size_t origIdx = (size_t)colIdxs[c];
        int w = allWidths[origIdx];
        if (x + w > screenW) w = screenW - x;
        dispIdxsArr[nVisCols] = c;
        xs[nVisCols] = x;
        ws[nVisCols] = w;
        nVisCols++;
        x += w + 1;
    }

    /* expand cursor column to absorb trailing screen whitespace */
    if (nVisCols > 0) {
        size_t last = nVisCols - 1;
        int usedW = xs[last] + ws[last] + 1;
        int slack = screenW - usedW;
        if (slack > 0) {
            size_t curVisIdx = nVisCols;
            for (size_t c = 0; c < nVisCols; c++) {
                size_t origIdx = (size_t)colIdxs[dispIdxsArr[c]];
                if (origIdx == curCol) { curVisIdx = c; break; }
            }
            if (curVisIdx < nVisCols) {
                size_t origIdx = (size_t)colIdxs[dispIdxsArr[curVisIdx]];
                int base = baseWidths[origIdx] + (int)widthAdj;
                if (base < 3) base = 3;
                int expand = base - ws[curVisIdx];
                if (expand > slack) expand = slack;
                if (expand > 0) {
                    ws[curVisIdx] += expand;
                    for (size_t c = curVisIdx + 1; c < nVisCols; c++)
                        xs[c] += expand;
                }
            }
        }
    }

    /* ---- header + footer with separators and type chars ---- */
    int yFoot = screenH - 3;
    for (size_t c = 0; c < nVisCols; c++) {
        size_t dispIdx = dispIdxsArr[c];
        size_t origIdx = (size_t)colIdxs[dispIdx];
        const char *name = names[origIdx] ? names[origIdx] : "";
        int isSel = IS_SEL(colBits, origIdx);
        int isCur = (origIdx == curCol);
        int isGrp = (dispIdx < nKeys);
        int si = isCur ? STYLE_CURSOR
                       : (isSel ? STYLE_SEL_COL
                                : (isGrp ? STYLE_GROUP : STYLE_HEADER));
        uint32_t fg = stFg[si] | TB_BOLD | TB_UNDERLINE;
        uint32_t bg = isGrp ? stBg[STYLE_GROUP] : stBg[si];

        set_cell(&ctx, xs[c], 0,      ' ', fg, bg);
        set_cell(&ctx, xs[c], yFoot,  ' ', fg, bg);

        int hw = ws[c] > 2 ? ws[c] - 2 : 0;
        if (hw > 0) {
            print_pad(&ctx, xs[c] + 1, 0,     hw, fg, bg, name, 0);
            print_pad(&ctx, xs[c] + 1, yFoot, hw, fg, bg, name, 0);
        }

        char tc = (origIdx < nFmts && fmts)
                    ? type_char_fmt(fmts[origIdx])
                    : type_char_col(&cols[origIdx]);
        set_cell(&ctx, xs[c] + ws[c] - 1, 0,     tc, fg, bg);
        set_cell(&ctx, xs[c] + ws[c] - 1, yFoot, tc, fg, bg);

        int sX = xs[c] + ws[c];
        if (sX < screenW) {
            int isKey = (c + 1 == visKeys);
            uint32_t sc = isKey ? 0x2551 : 0x2502;
            uint32_t sf = isKey ? stFg[STYLE_GROUP] : SEP_FG;
            set_cell(&ctx, sX, 0,     sc, sf, stBg[STYLE_DEFAULT]);
            set_cell(&ctx, sX, yFoot, sc, sf, stBg[STYLE_DEFAULT]);
        }
    }

    /* ---- sparkline row (y=1) ---- */
    if (sparkOn) {
        uint32_t spFg = stFg[STYLE_HEADER];
        uint32_t spBg = stBg[STYLE_DEFAULT];
        for (size_t c = 0; c < nVisCols; c++) {
            size_t dispIdx = dispIdxsArr[c];
            size_t origIdx = (size_t)colIdxs[dispIdx];
            const char *sp = (origIdx < nSparklines && sparklines[origIdx]) ? sparklines[origIdx] : "";
            print_pad(&ctx, xs[c], 1, ws[c], spFg, spBg, sp, 0);
            int sX = xs[c] + ws[c];
            if (sX < screenW) {
                int isKey = (c + 1 == visKeys);
                uint32_t sc = isKey ? 0x2551 : 0x2502;
                uint32_t sf = isKey ? stFg[STYLE_GROUP] : SEP_FG;
                set_cell(&ctx, sX, 1, sc, sf, stBg[STYLE_DEFAULT]);
            }
        }
    }

    int dataY0 = sparkOn ? 2 : 1;

    HeatCol hcols[MAX_HEAT_COLS];
    memset(hcols, 0, sizeof(hcols));
    if (heatMode && nVisCols <= MAX_HEAT_COLS)
        heat_scan(cols, colIdxs, dispIdxsArr, nVisCols, nRows, r0, fmts, nFmts, hcols);

    /* ---- data rows ---- */
    for (size_t ri = 0; ri < nRows; ri++) {
        uint64_t row = r0 + ri;
        int y = (int)ri + dataY0;
        int isSelRow = IS_SEL(rowBits, row);
        int isCurRow = (row == curRow);

        for (size_t c = 0; c < nVisCols; c++) {
            size_t dispIdx = dispIdxsArr[c];
            size_t origIdx = (size_t)colIdxs[dispIdx];
            int isSel = IS_SEL(colBits, origIdx);
            int isCurCol = (origIdx == curCol);
            int isGrp = (dispIdx < nKeys);
            int si = get_style(isCurRow && isCurCol, isSelRow, isSel, isCurRow, isCurCol);
            uint32_t bg = isGrp ? stBg[STYLE_GROUP] : stBg[si];
            uint32_t fg = stFg[si];

            const TvCol *col = &cols[origIdx];
            if (heatMode && heat_cell_bg(col, row, c, si, heatMode, hcols, &bg))
                fg = HEAT_FG;
            format_col_cell(col, (size_t)row, buf, sizeof(buf), (int)prec);

            set_cell(&ctx, xs[c], y, ' ', fg, bg);
            int cw = ws[c] > 2 ? ws[c] - 2 : 0;
            if (cw > 0) print_pad(&ctx, xs[c] + 1, y, cw, fg, bg, buf, col_is_num(col));
            set_cell(&ctx, xs[c] + ws[c] - 1, y, ' ', fg, bg);

            int sX = xs[c] + ws[c];
            if (sX < screenW) {
                int isKey = (c + 1 == visKeys);
                uint32_t sc = isKey ? 0x2551 : 0x2502;
                uint32_t sf = isKey ? stFg[STYLE_GROUP] : SEP_FG;
                set_cell(&ctx, sX, y, sc, sf, stBg[STYLE_DEFAULT]);
            }
        }
    }

    /* ---- tooltip: full header name if truncated ---- */
    for (size_t c = 0; c < nVisCols; c++) {
        size_t dispIdx = dispIdxsArr[c];
        size_t origIdx = (size_t)colIdxs[dispIdx];
        if (origIdx != curCol) continue;
        const char *name = names[origIdx] ? names[origIdx] : "";
        int nameLen = (int)utf8_len(name, 256);
        int colW = ws[c] - 2;
        if (nameLen <= colW) break;
        uint32_t fg = stFg[STYLE_CURSOR] | TB_BOLD | TB_UNDERLINE;
        const char *p = name;
        if (moveDir > 0) {
            int endX = xs[c] + ws[c] - 1;
            int startX = endX - nameLen;
            if (startX < 0) startX = 0;
            int tipW = endX - startX;
            int skip = nameLen - tipW;
            for (int i = 0; i < skip && *p; i++) utf8_decode(&p);
            for (int i = 0; i < tipW && *p; i++)
                set_cell(&ctx, startX + i, 0, utf8_decode(&p), fg, stBg[STYLE_CURSOR]);
        } else {
            int maxW = screenW - xs[c] - 1;
            int tipW = nameLen < maxW ? nameLen : maxW;
            for (int i = 0; i < tipW && *p; i++)
                set_cell(&ctx, xs[c] + 1 + i, 0, utf8_decode(&p), fg, stBg[STYLE_CURSOR]);
        }
        break;
    }

    /* output widths (no widthAdj, 0 for hidden) */
    for (size_t c = 0; c < nCols; c++) outWidths[c] = (uint32_t)baseWidths[c];

    free(dispIdxsArr);
    free(xs);
    free(ws);
    free(baseWidths);
    free(allWidths);
}
