package com.bigspark.cloudera.management.common.metadata;

import com.bigspark.cloudera.management.common.model.TableDescriptor;

public class PurgingMetadata {

  public final String database;
  public final String tableName;
  public final TableDescriptor tableDescriptor;
  public final Integer retentionPeriod;
  public final boolean isRetainMonthEnd;

  public PurgingMetadata(String database, String tableName, Integer retentionPeriod,
      boolean isRetainMonthEnd, TableDescriptor tableDescriptor) {
    this.database = database;
    this.tableName = tableName;
    this.retentionPeriod = retentionPeriod;
    this.isRetainMonthEnd = isRetainMonthEnd;
    this.tableDescriptor = tableDescriptor;
  }


}
