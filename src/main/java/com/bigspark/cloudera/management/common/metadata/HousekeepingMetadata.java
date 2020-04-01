package com.bigspark.cloudera.management.common.metadata;

import com.bigspark.cloudera.management.common.model.TableDescriptor;

public class HousekeepingMetadata {
    public String database;
    public String tableName;
    public TableDescriptor tableDescriptor;
    public Integer retentionPeriod;
    public boolean isRetainMonthEnd;

    public HousekeepingMetadata(String database, String tableName, Integer retentionPeriod, boolean isRetainMonthEnd, TableDescriptor tableDescriptor) {
        this.database = database;
        this.tableName = tableName;
        this.retentionPeriod = retentionPeriod;
        this.isRetainMonthEnd = isRetainMonthEnd;
        this.tableDescriptor = tableDescriptor;
    }


}
