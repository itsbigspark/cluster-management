package dev.bigspark.cloudera.management.common.metadata;

import dev.bigspark.model.TableDescriptor;
import org.apache.hadoop.fs.Path;

public class CompactionMetadata {

  public String database;
  public String tableName;
  public TableDescriptor tableDescriptor;
  public Path path;

  public CompactionMetadata(TableDescriptor tableDescriptor) {
    this.database = tableDescriptor.getDatabaseName();
    this.tableName = tableDescriptor.getTableName();
    this.tableDescriptor = tableDescriptor;
  }

  public CompactionMetadata(Path path) {
    this.path = path;
  }

}
