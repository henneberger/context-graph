package io.contextgraph.processor;

import java.util.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.*;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.flink.CatalogLoader;

/** Offline writer cutover: retain the entire old warehouse; rewrite live rows and verify before resuming. */
public final class MigrateWarehouse {
  record Fingerprint(long rows,String digest) {}
  static Fingerprint fingerprint(Table table)throws Exception {
    long count=0;BigInteger sum=BigInteger.ZERO,mask=BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
    try(var rows=IcebergGenerics.read(table).build()) {for(var row:rows){sum=sum.add(new BigInteger(1,MessageDigest.getInstance("SHA-256").digest(row.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8)))).and(mask);count++;}}
    return new Fingerprint(count,sum.toString(16));
  }
  public static void main(String[] args)throws Exception {
    if(!"true".equals(System.getenv("MIGRATION_WRITER_SUSPENDED")))throw new IllegalStateException("Suspend Flink at a completed savepoint before migrating");
    var source=CatalogLoader.hadoop("legacy",new org.apache.hadoop.conf.Configuration(),Map.of("warehouse","file:///data/warehouse")).loadCatalog();
    var target=IcebergTables.loader("context").loadCatalog();
    for(String kind:List.of("events","nodes","edges","metrics")) {
      var id=TableIdentifier.of("context_secure",kind);Table old=source.loadTable(id);long snapshot=old.currentSnapshot().snapshotId();
      Table table=target.tableExists(id)?target.loadTable(id):target.createTable(id,old.schema(),PartitionSpec.unpartitioned(),Map.of("format-version","2","context.migration.source-snapshot",Long.toString(snapshot)));
      if(!Long.toString(snapshot).equals(table.properties().get("context.migration.source-snapshot")))throw new IllegalStateException("Source changed: "+kind);
      Fingerprint before=fingerprint(old);
      if(table.currentSnapshot()==null) {
        String location=table.location()+"/data/migration-"+UUID.randomUUID()+".parquet";
        var writer=new GenericAppenderFactory(table.schema(),table.spec()).newDataWriter(EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(location)),FileFormat.PARQUET,null);
        try(writer;var rows=IcebergGenerics.read(old).useSnapshot(snapshot).build()){for(var row:rows)writer.write(row);}
        var append=table.newAppend().appendFile(writer.toDataFile());
        // Preserve source checkpoint commit markers for the restored Flink sink.
        old.currentSnapshot().summary().forEach((key,value)->{if(key.startsWith("flink."))append.set(key,value);});
        append.set("context.migration.source-snapshot",Long.toString(snapshot)).commit();
      }
      table.refresh();Fingerprint after=fingerprint(table);
      if(!before.equals(after))throw new IllegalStateException("Migration fingerprint mismatch: "+kind);
      if(table.spec().isUnpartitioned())table.updateSpec().addField(org.apache.iceberg.expressions.Expressions.day(kind.equals("metrics")?"window_start":"event_time")).commit();
      table.updateProperties().set("context.migration.verified","true").set("context.migration.rows",Long.toString(before.rows())).set("context.migration.digest",before.digest()).commit();
      System.out.println("VERIFIED "+kind+" rows="+before.rows()+" sourceSnapshot="+snapshot+" digest="+before.digest());
    }
  }
}
