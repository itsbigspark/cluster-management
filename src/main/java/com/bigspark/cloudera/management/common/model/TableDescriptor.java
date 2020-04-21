package com.bigspark.cloudera.management.common.model;


import com.bigspark.cloudera.management.common.exceptions.SourceException;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Created by chris on 16/12/2019.
 */
public class TableDescriptor {

  private final Table table;
  private final String databaseName;
  private final String tableName;
  private final Boolean isPartitioned;
  List<Partition> partitionList;

  public TableDescriptor(Table table, String databaseName, String tableName,
      Boolean isPartitioned) {
    this.table = table;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.isPartitioned = isPartitioned;
  }

  public TableDescriptor(Table table, String databaseName, String tableName, Boolean isPartitioned,
      List<Partition> partitionList) {
    this.table = table;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.isPartitioned = isPartitioned;
    this.partitionList = partitionList;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public Boolean isPartitioned() {
    return isPartitioned;
  }

  public Boolean hasPartitions() {
    if(this.partitionList != null && this.partitionList.size() > 0 ) {
      return true;
    }
    return false;
  }

  public Table getTable() {
    return table;
  }

  public List<Partition> getPartitionList() throws SourceException {
    if (isPartitioned && partitionList.size() > 0) {
      return partitionList;
    } else if (partitionList.size() == 0) {
      throw new SourceException("No partitions located on table: " + tableName);
    } else {
      throw new SourceException("Table: " + tableName + " is not partitioned");
    }
  }


}
