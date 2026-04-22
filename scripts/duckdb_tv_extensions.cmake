# tv-hask minimal DuckDB extension config.
#
# This supplements extension/extension_config.cmake (core_functions, parquet)
# with the two extra extensions tv actually uses (icu, json). jemalloc is
# dropped via -DSKIP_EXTENSIONS=jemalloc on the cmake command line, and we
# avoid DuckDB's .github/config/bundled_extensions.cmake entirely so that
# autocomplete, tpch, tpcds do not get pulled in.

duckdb_extension_load(icu)
duckdb_extension_load(json)
