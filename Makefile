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

.PHONY: prqlc clean-prqlc

prqlc: $(PRQL_LIB)

$(PRQL_LIB): $(PRQL_BUILT)
	mkdir -p vendor/lib
	cp -f $< $@

$(PRQL_BUILT): | $(PRQL_DIR)
	cd $(PRQL_DIR)/prqlc/bindings/prqlc-c && cargo build --release

$(PRQL_DIR):
	mkdir -p vendor
	git clone --depth 1 --branch $(PRQL_REF) $(PRQL_REPO) $@

clean-prqlc:
	rm -rf $(PRQL_DIR)/target vendor/lib
