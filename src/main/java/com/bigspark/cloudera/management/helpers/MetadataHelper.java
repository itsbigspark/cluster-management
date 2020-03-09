package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.configuration.HiveConfiguration;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by chris on 12/12/2019.
 */
public class MetadataHelper {


    Logger logger = LoggerFactory.getLogger(getClass());
    private static HiveMetaStoreClient client;

    public MetadataHelper() throws MetaException, ConfigurationException {
    }


    public TableDescriptor getTableDescriptor(Table table) throws SourceException {
        if (table.getPartitionKeys().size() == 0) {
            return new TableDescriptor(
                    table.getDbName()
                    , table.getTableName()
                    , false);

        } else {
            return new TableDescriptor(
                    table.getDbName()
                    , table.getTableName()
                    , true
                    , getTablePartitions(table.getDbName(),table.getTableName()));
        }
    }


    public ArrayList<TableDescriptor> getAllTableDescriptors(List<Table> tableList) throws SourceException {
        ArrayList<TableDescriptor> tableDescriptors = Lists.newArrayList();
        for (Table table:tableList) {
            tableDescriptors.add(getTableDescriptor(table));
        }
        return tableDescriptors;
    }

    public Table getTable(String database, String table) throws SourceException {
        Table t;
        try {
            t = client.getTable(database, table);
        } catch (TException e) {
            throw new SourceException("Table not found", e.getCause());
        }
        return t;
    }


    public ArrayList<Table> getAllTablesFromDatabase(String database) throws SourceException {
        List<String> tableNames;
        try {
            tableNames = client.getAllTables(database);
        } catch (TException e) {
            throw new SourceException("Table not found", e.getCause());
        }

        ArrayList<Table> tableList = Lists.newArrayList();
        for (String table:tableNames){
            tableList.add(getTable(database,table));
        }
        return tableList;
    }

    public List<Partition> getTablePartition(String dbName, String tableName, String partitionName) throws SourceException {
        List<Partition> partitions = Lists.newArrayList();
        try {
            Partition p = client.getPartition(dbName,tableName,partitionName);
            partitions.add(p);
        } catch (TException e) {
            throw new SourceException("Partition not found", e.getCause());
        }
        return partitions;
    }

    public List<Partition> getTablePartitions(String dbName, String tableName) throws SourceException{
        List<Partition> partitions;
        try {
            partitions = client.listPartitions(dbName,tableName, (short) 10000);
        } catch (TException e) {
            throw new SourceException("Error retrieving partitions", e.getCause());
        }
        return partitions;
    }


    public Database getDatabase(String database) throws SourceException {
        Database d;
        try {
            d = client.getDatabase(database);
        } catch (TException e) {
            throw new SourceException("Database not found", e.getCause());
        }
        return d;
    }

    public List<String> getAllDatabases() throws MetaException {
         return client.getAllDatabases();
    }

    public String getPartitionDateString(Partition partition) {
        //SH - /prod/source-history/ADB/ADB_BRANCH/edi_business_day=2020-01-20/
        //EAS - /prod/enterprise-analytics-store/data/AGREEMENT/edi_business_day=2020-01-20/src_sys_id=ADB/src_sys_inst_id=NWB/
        for (int i = 0; i <= partition.getValues().size(); i = i + 1) {
            if (partition.getValues().get(i).contains("edi_business_day")){
                logger.debug(String.format("Date key found at level %s/%s for table: %s.%s, partition: %s"
                        ,i,partition.getValues().size(),partition.getDbName(),partition.getTableName(),partition.toString()));
                return partition.getValues().get(i).split("=")[1];
            }
        }
        logger.debug("No date found in partition keys, returning null");
        return null;
    }

    public Date getPartitionDate(Partition partition) {
        String partitionDateString = getPartitionDateString(partition);
        return stringToDate(partitionDateString);
    }

    public Date stringToDate (String dateStr){
        DateWritable writableVal = new DateWritable(java.sql.Date.valueOf(dateStr));
        return new Date(writableVal.getDays());
    }


}
