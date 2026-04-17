# tv-hask

Haskell port of [Tc](https://github.com/co-dh/Tc) — a VisiData-style terminal
table viewer with a DuckDB backend. Opens CSV, Parquet, JSON, Arrow, DuckDB,
SQLite, and gzip files; browses S3 buckets, HuggingFace datasets, FTP
servers, osquery tables, and Postgres databases.

For features and key bindings, see the Lean project's
[README](https://github.com/co-dh/Tc#readme) — output is cast-identical.

## Dependencies

Runtime:

| Tool        | Purpose                            | Required |
|-------------|------------------------------------|----------|
| `libduckdb` | Query engine (loaded via FFI)      | yes      |
| `prqlc`     | PRQL → SQL compiler (shelled out)  | yes      |
| `Rscript`   | Plot rendering (ggplot2)           | optional |
| `kitty`     | Plot image display                 | optional |
| `viu`       | Plot image fallback (non-kitty)    | optional |
| `osqueryi`  | `osquery://` source                | optional |
| `aws`       | `s3://` source                     | optional |
| `curl`      | `ftp://`, `hf://`, `rest://`       | optional |

Build-time: a Haskell toolchain — see below.

On Arch Linux:

```bash
sudo pacman -S duckdb prqlc r
# optional:
sudo pacman -S kitty viu
# optional for extensions:
yay -S osquery aws-cli-v2
```

Other distros: install `libduckdb.so` via the
[DuckDB release tarball](https://github.com/duckdb/duckdb/releases) (copy to
`/usr/local/lib` and `ldconfig`); install `prqlc` from its
[releases](https://github.com/PRQL/prql/releases).

## Build from source

**Use `ghcup`, not your distro's `ghc` package.** On Arch in particular, the
pacman `ghc` package ships only dynamic interface files (`.dyn_hi`), while
cabal's source builds of Hackage deps default to looking for static ones
(`.hi`). That mismatch produces "Could not find module 'Data.Hashable'"
errors for packages that actually built fine. `ghcup`'s toolchain lives
under `~/.ghcup/`, contains both flavors, and doesn't collide with pacman's
`haskell-*` packages.

Install the toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://get-ghcup.haskell.org | sh
ghcup install ghc 9.6.6
ghcup install cabal
ghcup set ghc 9.6.6
```

Then clone and build:

```bash
git clone https://github.com/co-dh/tv-hask
cd tv-hask
cabal build all        # produces dist-newstyle/.../tv
cabal install          # copies `tv` to ~/.cabal/bin (add to $PATH)
```

Run tests:

```bash
cabal test             # 169 tasty + 97 doctest
```

## Binary releases

No pre-built binaries yet — build from source for now. (The Lean
[`Tc`](https://github.com/co-dh/Tc/releases) project has static binaries if
you want a turn-key install without a Haskell toolchain.)

## Run

```bash
tv data.csv                        # CSV file
tv data.parquet                    # Parquet file
tv data.duckdb                     # DuckDB file (lists tables)
tv .                               # Browse current directory
tv s3://bucket/prefix/             # Browse S3 bucket
tv s3://bucket/prefix/ +n          # Public bucket (no credentials)
tv hf://datasets/user/dataset      # HuggingFace dataset
tv ftp://ftp.nyse.com/             # FTP listing
tv osquery://                      # osquery tables
tv 'pg://host=/run/postgresql'     # Postgres DSN
cat data.csv | tv                  # Pipe mode
tv -s mysession                    # Restore saved session
```

Press `Space` inside the TUI to open the command menu.

## Project layout

See [doc/architecture.md](doc/architecture.md) for the main loop, module
layering, and invariants.
