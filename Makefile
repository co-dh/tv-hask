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

# DuckDB ships prebuilt staticlib bundles on every release; download
# instead of building from source (~30-60min, ~5GB disk).
# Pinned to v1.5.2 to match duckdb-ffi 1.5.x ABI; bumping risks ABI drift.
DUCKDB_REPO ?= duckdb/duckdb
DUCKDB_TAG  ?= v1.5.2
DUCKDB_DIR  := vendor/duckdb
DUCKDB_LIB  := $(DUCKDB_DIR)/libduckdb.a

# Map host platform → DuckDB release asset name. DuckDB only ships
# binaries for the four below; anything else needs a source build.
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S)-$(UNAME_M),Linux-x86_64)
  DUCKDB_ASSET := static-libs-linux-amd64.zip
else ifeq ($(UNAME_S)-$(UNAME_M),Linux-aarch64)
  DUCKDB_ASSET := static-libs-linux-arm64.zip
else ifeq ($(UNAME_S)-$(UNAME_M),Darwin-x86_64)
  DUCKDB_ASSET := static-libs-osx-amd64.zip
else ifeq ($(UNAME_S)-$(UNAME_M),Darwin-arm64)
  DUCKDB_ASSET := static-libs-osx-arm64.zip
else
  DUCKDB_ASSET := UNSUPPORTED
endif

.PHONY: prqlc duckdb clean-prqlc clean-duckdb clean-duckdb-cache

prqlc: $(PRQL_LIB)
duckdb: check-duckdb-platform $(DUCKDB_LIB)

check-duckdb-platform:
ifeq ($(DUCKDB_ASSET),UNSUPPORTED)
	@echo "ERROR: no prebuilt DuckDB static-libs for $(UNAME_S)/$(UNAME_M)."
	@echo "Supported: Linux-x86_64, Linux-aarch64, Darwin-x86_64, Darwin-arm64."
	@echo "Build duckdb from source with -DBUILD_STATIC_LIB=ON and place the"
	@echo "resulting .a files in $(DUCKDB_DIR)/."
	@exit 1
endif

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

# .fetched depends on the zip — keeps the zip cached across
# `clean-duckdb` so re-extracting doesn't re-download 30 MB.
$(DUCKDB_DIR)/.fetched: $(DUCKDB_DIR)/$(DUCKDB_ASSET)
	cd $(DUCKDB_DIR) && unzip -o $(DUCKDB_ASSET)
	touch $@

$(DUCKDB_DIR)/$(DUCKDB_ASSET):
	mkdir -p $(DUCKDB_DIR)
	cd $(DUCKDB_DIR) && gh release download $(DUCKDB_TAG) \
		--repo $(DUCKDB_REPO) \
		--pattern $(DUCKDB_ASSET) \
		--clobber

clean-prqlc:
	rm -rf $(PRQL_DIR)/target vendor/lib

# Removes extracted .a files + sentinel but keeps the zip cached. Use
# clean-duckdb-cache to also discard the downloaded zip. find -mindepth
# is needed to catch the hidden .fetched marker that ls misses.
clean-duckdb:
	-find $(DUCKDB_DIR) -mindepth 1 -maxdepth 1 ! -name '*.zip' -exec rm -rf {} +

clean-duckdb-cache: clean-duckdb
	rm -rf $(DUCKDB_DIR)
