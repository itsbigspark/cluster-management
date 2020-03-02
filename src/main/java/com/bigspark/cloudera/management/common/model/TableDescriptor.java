package com.bigspark.cloudera.management.common.model;


import com.bigspark.cloudera.management.common.exceptions.SourceException;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.List;

/**
 * Created by chris on 16/12/2019.
 */
public class TableDescriptor {
    String databaseName;
    String tableName;
    Boolean isPartitioned;
    List<Partition> partitionList;

    public TableDescriptor(String databaseName, String tableName, Boolean isPartitioned) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.isPartitioned = isPartitioned;
    }

    public TableDescriptor(String databaseName, String tableName, Boolean isPartitioned, List<Partition> partitionList) {
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

    public List<Partition> getPartitionList() throws SourceException {
        if (isPartitioned && partitionList.size()>0) {
            return partitionList;
        } else if (partitionList.size()==0){
            throw new SourceException("No partitions located on table: "+tableName);
        } else {
            throw new SourceException("Table: "+tableName+" is not partitioned");
        }
    }


}
