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

# DuckDB ships prebuilt staticlib bundles on every release; `make duckdb`
# downloads those. `make duckdb-source` builds from source with only the
# extensions tv uses (shrinks tv binary by dropping autocomplete/tpch/tpcds/
# jemalloc, saves ~6-8 MB after --gc-sections) at the cost of a 30-60min
# first build. Pinned to v1.5.2 to match duckdb-ffi 1.5.x ABI.
DUCKDB_REPO ?= duckdb/duckdb
DUCKDB_TAG  ?= v1.5.2
DUCKDB_DIR  := vendor/duckdb
DUCKDB_LIB  := $(DUCKDB_DIR)/libduckdb.a
DUCKDB_SRC  := vendor/duckdb-src
# Extension config lives under scripts/ (checked into git). The cmake file
# supplements DuckDB's default extension/extension_config.cmake (which
# already loads core_functions + parquet) by adding icu + json. jemalloc
# is force-loaded for linux-x86_64 in the default config, so we drop it
# via SKIP_EXTENSIONS. autocomplete/tpch/tpcds are only pulled in by the
# stock bundled_extensions.cmake — we don't reference it, so they stay
# out.
DUCKDB_EXT_CFG := $(abspath scripts/duckdb_tv_extensions.cmake)

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

.PHONY: prqlc duckdb duckdb-source clean-prqlc clean-duckdb clean-duckdb-cache clean-duckdb-src

prqlc: $(PRQL_LIB)
duckdb: check-duckdb-platform $(DUCKDB_LIB)
# duckdb-source overwrites the prebuilt archives in vendor/duckdb/ with a
# source build containing only {core_functions, parquet, icu, json}.
duckdb-source: $(DUCKDB_DIR)/.source-built

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

clean-duckdb-src:
	rm -rf $(DUCKDB_SRC)

# ---- DuckDB source build (minimal extensions) --------------------------
#
# This rebuilds libduckdb_static.a + the 4 needed extension archives from
# a shallow clone at $(DUCKDB_TAG), then copies them into $(DUCKDB_DIR)/
# (overwriting whatever `make duckdb` put there). The cabal link lines
# still reference the same -lduckdb / -l<ext>_extension names, so no
# cabal edits are needed for the base link — just drop the unused
# -l<ext>_extension flags for extensions that no longer exist.
#
# Option mapping (validated against vendor/duckdb-src/CMakeLists.txt):
#   BUILD_SHELL=0               skip the duckdb CLI (we link the lib)
#   BUILD_UNITTESTS=0           skip duckdb's own tests
#   ENABLE_EXTENSION_AUTOLOADING=0 / AUTOINSTALL=0 — match release config
#   ENABLE_SANITIZER=0 / ENABLE_UBSAN=0 — release build should be clean
#   SMALLER_BINARY=1            — trims specialized fast paths; tv is
#     interactive and the trimmed paths are numeric hot loops we don't hit.
#   DUCKDB_EXTENSION_CONFIGS=<cfg>  — our scripts/duckdb_tv_extensions.cmake
#     adds icu + json on top of the built-in core_functions + parquet.
#   SKIP_EXTENSIONS=jemalloc    — linux default config force-loads it;
#     overriding via SKIP_EXTENSIONS keeps the bypass local.
$(DUCKDB_DIR)/.source-built: $(DUCKDB_SRC)/build/release/.built | $(DUCKDB_DIR)
	mkdir -p $(DUCKDB_DIR)
	cp -f $(DUCKDB_SRC)/build/release/src/libduckdb_static.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/extension/libduckdb_generated_extension_loader.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/extension/core_functions/libcore_functions_extension.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/extension/parquet/libparquet_extension.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/extension/icu/libicu_extension.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/extension/json/libjson_extension.a $(DUCKDB_DIR)/
	cp -f $(DUCKDB_SRC)/build/release/third_party/*/libduckdb_*.a $(DUCKDB_DIR)/
	rm -f $(DUCKDB_DIR)/libautocomplete_extension.a \
	      $(DUCKDB_DIR)/libtpch_extension.a \
	      $(DUCKDB_DIR)/libtpcds_extension.a \
	      $(DUCKDB_DIR)/libjemalloc_extension.a
	ln -sf libduckdb_static.a $(DUCKDB_LIB)
	touch $@

$(DUCKDB_DIR):
	mkdir -p $@

$(DUCKDB_SRC)/build/release/.built: | $(DUCKDB_SRC)
	cd $(DUCKDB_SRC) && cmake -B build/release \
	  -DCMAKE_BUILD_TYPE=Release \
	  -DBUILD_SHELL=0 \
	  -DBUILD_UNITTESTS=0 \
	  -DENABLE_EXTENSION_AUTOLOADING=0 \
	  -DENABLE_EXTENSION_AUTOINSTALL=0 \
	  -DENABLE_SANITIZER=0 \
	  -DENABLE_UBSAN=0 \
	  -DSMALLER_BINARY=1 \
	  -DDUCKDB_EXTENSION_CONFIGS=$(DUCKDB_EXT_CFG) \
	  -DSKIP_EXTENSIONS=jemalloc
	cmake --build $(DUCKDB_SRC)/build/release -j$$(nproc)
	touch $@

$(DUCKDB_SRC):
	mkdir -p vendor
	git clone --depth 1 --branch $(DUCKDB_TAG) https://github.com/$(DUCKDB_REPO).git $@
