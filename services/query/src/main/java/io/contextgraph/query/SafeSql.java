package io.contextgraph.query;

import java.util.*;
import java.util.regex.*;

/** Restricted trusted query language; external access is also disabled by DuckDB. */
final class SafeSql {
  private static final Set<String> FUNCTIONS=Set.of("COUNT","SUM","AVG","MIN","MAX","ROW_NUMBER","RANK","DENSE_RANK","COALESCE","NULLIF","CAST","TRY_CAST","ROUND","ABS","FLOOR","CEIL","LOWER","UPPER","LENGTH","DATE_TRUNC","TIME_BUCKET","EXTRACT","GREATEST","LEAST","OVER","PARTITION","FILTER","IN","AS","EXCLUDE","FROM","SELECT","WHERE","AND","OR","NOT","ON","HAVING","WHEN");
  static void validate(String sql) { validate(sql,Set.of("source")); }
  static void validate(String sql,Set<String> allowedSources) {
    if(sql==null||sql.length()>32000||sql.contains(";")||sql.contains("--")||sql.contains("/*")||sql.contains("\"")) throw new IllegalArgumentException("Only a single restricted SELECT/CTE is allowed");
    String plain=sql.replaceAll("'(?:''|[^'])*'", "''");
    if(!plain.stripLeading().toUpperCase(Locale.ROOT).matches("(?s)^(SELECT|WITH)\\b.*")) throw new IllegalArgumentException("Only SELECT/CTE queries are allowed");
    if(Pattern.compile("(?i)\\b(ATTACH|COPY|EXPORT|IMPORT|INSTALL|LOAD|PRAGMA|CALL|SET|CREATE|DROP|DELETE|UPDATE|INSERT|ALTER|SECRET|SYSTEM|READ_CSV|READ_PARQUET|ICEBERG_SCAN)\\b").matcher(plain).find()) throw new IllegalArgumentException("Unsafe SQL construct");
    if(Pattern.compile("(?i)\\b(?:FROM|JOIN)\\s+[a-z_][a-z0-9_]*\\s*\\.").matcher(plain).find())throw new IllegalArgumentException("Qualified relations are not allowed");
    Set<String> sources=new HashSet<>(allowedSources.stream().map(s->s.toUpperCase(Locale.ROOT)).toList());
    Matcher cte=Pattern.compile("(?i)\\b([a-z_][a-z0-9_]*)\\s+AS\\s*\\(").matcher(plain);
    while(cte.find()) sources.add(cte.group(1).toUpperCase(Locale.ROOT));
    Matcher table=Pattern.compile("(?i)\\b(?:FROM|JOIN)\\s+([a-z_][a-z0-9_]*)").matcher(plain);
    while(table.find()) if(!sources.contains(table.group(1).toUpperCase(Locale.ROOT))) throw new IllegalArgumentException("Query may read only source or its CTEs");
    Matcher function=Pattern.compile("(?i)\\b([a-z_][a-z0-9_]*)\\s*\\(").matcher(plain);
    while(function.find()) if(!FUNCTIONS.contains(function.group(1).toUpperCase(Locale.ROOT))) throw new IllegalArgumentException("Unsupported SQL function: "+function.group(1));
  }
}
