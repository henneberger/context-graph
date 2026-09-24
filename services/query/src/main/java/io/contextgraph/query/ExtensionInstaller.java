package io.contextgraph.query;
/** Build-time installation ensures unprivileged/offline runtime can load extensions. */
public final class ExtensionInstaller {
  public static void main(String[] args) throws Exception {
    try(var connection=IcebergQueries.connect(true);var statement=connection.createStatement();var rows=statement.executeQuery("SELECT count(*) FROM duckdb_extensions() WHERE extension_name IN ('iceberg','cache_httpfs','fts') AND loaded")) {
      if(!rows.next()||rows.getInt(1)!=3)throw new IllegalStateException("Required DuckDB extensions did not load");
    }
  }
}
