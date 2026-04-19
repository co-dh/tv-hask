# Build the Rust prqlc-c staticlib that tv links against.
# `make prqlc` clones the PRQL repo into vendor/prql/ (if missing) and
# runs cargo build --release. The resulting libprqlc_c.a lands where
# tv-hask.cabal expects it (vendor/prql/target/release/).
#
# To use a local checkout instead of cloning, symlink it first:
#   ln -s ~/repo/prql vendor/prql
# then run `make prqlc`.

PRQL_REPO  ?= https://github.com/PRQL/prql
PRQL_REF   ?= 0.13.11
PRQL_DIR   := vendor/prql
PRQL_BUILT := $(PRQL_DIR)/target/release/libprqlc_c.a
# Cabal's extra-lib-dirs points at vendor/lib/ — a dir that contains the
# .a and nothing else. Pointing at target/release/ directly would let ld
# pick the co-located .so (it prefers dynamic) and break static linking.
PRQL_LIB   := vendor/lib/libprqlc_c.a

# DuckDB ships a prebuilt staticlib bundle on every release; download
# instead of building from source (full duckdb build is ~30-60min).
DUCKDB_REPO ?= duckdb/duckdb
DUCKDB_TAG  ?= latest
DUCKDB_DIR  := vendor/duckdb
DUCKDB_LIB  := $(DUCKDB_DIR)/libduckdb.a

.PHONY: prqlc duckdb clean-prqlc clean-duckdb

prqlc: $(PRQL_LIB)
duckdb: $(DUCKDB_LIB)

$(PRQL_LIB): $(PRQL_BUILT)
	mkdir -p vendor/lib
	cp -f $< $@

$(PRQL_BUILT): | $(PRQL_DIR)
	cd $(PRQL_DIR)/prqlc/bindings/prqlc-c && cargo build --release

$(PRQL_DIR):
	mkdir -p vendor
	git clone --depth 1 --branch $(PRQL_REF) $(PRQL_REPO) $@

# Symlink so `-lduckdb` finds the static archive (the release ships it
# as libduckdb_static.a; cabal's -lduckdb wants libduckdb.a).
$(DUCKDB_LIB): | $(DUCKDB_DIR)/.fetched
	ln -sf libduckdb_static.a $@

$(DUCKDB_DIR)/.fetched:
	mkdir -p $(DUCKDB_DIR)
	cd $(DUCKDB_DIR) && gh release download $(DUCKDB_TAG) \
		--repo $(DUCKDB_REPO) \
		--pattern "static-libs-linux-amd64.zip" \
		--clobber
	cd $(DUCKDB_DIR) && unzip -o static-libs-linux-amd64.zip
	touch $@

clean-prqlc:
	rm -rf $(PRQL_DIR)/target vendor/lib

clean-duckdb:
	rm -rf $(DUCKDB_DIR)
