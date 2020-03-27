package com.bigspark.cloudera.management.common.model;

public class TableMetadata {
    public String database;
    public String tableName;
    public TableDescriptor tableDescriptor;
    //Housekeeping
    public Integer retentionPeriod;
    public boolean isRetainMonthEnd;
    //Compaction


    public TableMetadata(String database, String tableName, Integer retentionPeriod, boolean isRetainMonthEnd, TableDescriptor tableDescriptor) {
        this.database = database;
        this.tableName = tableName;
        this.retentionPeriod = retentionPeriod;
        this.isRetainMonthEnd = isRetainMonthEnd;
        this.tableDescriptor = tableDescriptor;
    }

    public TableMetadata(String database, String tableName, TableDescriptor tableDescriptor) {
        this.database = database;
        this.tableName = tableName;
        this.tableDescriptor = tableDescriptor;
    }
}
