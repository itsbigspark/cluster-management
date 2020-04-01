package com.bigspark.cloudera.management.common.metadata;

import com.bigspark.cloudera.management.common.enums.Platform;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import org.apache.hadoop.fs.Path;

public class OffloadMetadata {
    public String database;
    public String tableName;
    public TableDescriptor tableDescriptor;
    public Platform platform;
    public String targetBucket;
    public Path sourcePath;

    public OffloadMetadata(TableDescriptor tableDescriptor, Platform platform, String targetBucket) {
        this.database = tableDescriptor.getDatabaseName();
        this.tableName = tableDescriptor.getTableName();
        this.tableDescriptor = tableDescriptor;
        this.platform = platform;
        this.targetBucket = targetBucket;
    }

    public OffloadMetadata(Path sourcePath, Platform platform, String targetBucket) {
        this.sourcePath = sourcePath;
        this.platform = platform;
        this.targetBucket = targetBucket;
    }

}
