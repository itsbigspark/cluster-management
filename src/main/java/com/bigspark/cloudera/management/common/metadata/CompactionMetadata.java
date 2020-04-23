package com.bigspark.cloudera.management.common.metadata;

import com.bigspark.cloudera.management.common.model.TableDescriptor;
import org.apache.hadoop.fs.Path;

public class CompactionMetadata {

  public String database;
  public String tableName;
  public TableDescriptor tableDescriptor;
  public Path path;
  public int constructor;

  public CompactionMetadata(TableDescriptor tableDescriptor) {
    this.database = tableDescriptor.getDatabaseName();
    this.tableName = tableDescriptor.getTableName();
    this.tableDescriptor = tableDescriptor;
    this.constructor = 1;
  }

  public CompactionMetadata(Path path) {
    this.path = path;
    this.constructor = 2;
  }

}
