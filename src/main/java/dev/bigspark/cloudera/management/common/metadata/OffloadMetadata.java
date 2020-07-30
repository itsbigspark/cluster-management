package dev.bigspark.cloudera.management.common.metadata;

import dev.bigspark.enums.Platform;
import dev.bigspark.model.TableDescriptor;
import org.apache.hadoop.fs.Path;

public class OffloadMetadata {

  public String database;
  public String tableName;
  public TableDescriptor tableDescriptor;
  public Platform platform;
  public String targetBucket;
  public Path sourcePath;
  public Integer hdfsRetention;
  public Integer constructor;

  public OffloadMetadata(TableDescriptor tableDescriptor, Platform platform, String targetBucket, Integer hdfsRetention) {
    this.constructor=1;
    this.database = tableDescriptor.getDatabaseName();
    this.tableName = tableDescriptor.getTableName();
    this.tableDescriptor = tableDescriptor;
    this.platform = platform;
    this.targetBucket = targetBucket;
    this.hdfsRetention = hdfsRetention;
  }

  public OffloadMetadata(Path sourcePath, Platform platform, String targetBucket) {
    this.constructor=2;
    this.sourcePath = sourcePath;
    this.platform = platform;
    this.targetBucket = targetBucket;
  }

  @Override
  public String toString() {
    return "OffloadMetadata{" +
        "database='" + database + '\'' +
        ", tableName='" + tableName + '\'' +
        ", tableDescriptor=" + tableDescriptor +
        ", platform=" + platform +
        ", targetBucket='" + targetBucket + '\'' +
        ", sourcePath=" + sourcePath +
        ", hdfsRetention=" + hdfsRetention +
        ", constructor=" + constructor +
        '}';
  }
}
