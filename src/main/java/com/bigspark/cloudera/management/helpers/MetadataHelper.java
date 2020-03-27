package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.configuration.HiveConfiguration;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.common.model.TableMetadata;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.bigspark.cloudera.management.common.enums.Pattern.EAS;
import static com.bigspark.cloudera.management.common.enums.Pattern.SH;

/**
 * Created by chris on 12/12/2019.
 */
public class MetadataHelper {


    private static Logger logger = LoggerFactory.getLogger(MetadataHelper.class);
    private static HiveMetaStoreClient client;

    public MetadataHelper() throws MetaException, ConfigurationException {
        this.client = getHiveMetastoreClient();
    }

    public HiveMetaStoreClient getHiveMetastoreClient() throws MetaException {
        if (this.client == null){
            return new HiveMetaStoreClient(new HiveConf());
        }
        return client;
    }

    public TableDescriptor getTableDescriptor(Table table) throws SourceException {
        if (table.getPartitionKeys().size() == 0) {
            return new TableDescriptor(
                    table
                    , table.getDbName()
                    , table.getTableName()
                    , false);

        } else {
            return new TableDescriptor(
                    table
                    , table.getDbName()
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
            throw new SourceException("Table not found : "+table, e.getCause());
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

    public String getPartitionDateString(Partition partition, Pattern pattern) {
        //SH - /prod/source-history/ADB/ADB_BRANCH/edi_business_day=2020-01-20/
        //EAS - /prod/enterprise-analytics-store/data/AGREEMENT/edi_business_day=2020-01-20/src_sys_id=ADB/src_sys_inst_id=NWB/
        String partitionLocation = partition.getSd().getLocation();
        String[] partitionLocationParts = partitionLocation.split("/");
        String partitionName = null;
        if (pattern == SH) {
            partitionName = partitionLocationParts[partitionLocationParts.length - 1];
        } else if (pattern == EAS){
            partitionName = partitionLocationParts[partitionLocationParts.length - 3];
        }
        return partitionName.split("=")[1];
    }

    public Date getPartitionDate(Partition partition, Pattern pattern) throws ParseException {
        String partitionDateString = getPartitionDateString(partition, pattern);
        return stringToDate(partitionDateString);
    }

    public Date stringToDate (String dateStr) throws ParseException {
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.parse(dateStr);
    }

    /**
     * Method to return the location defined on a Hive table partition
     * @param partition
     * @return
     */
    public String getPartitionLocation(Partition partition) {
        return partition.getSd().getLocation();
    }

    /**
     * Method to return the location defined on a Hive table
     * @param table
     * @return
     */
    public String getTableLocation(Table table) {
        return table.getSd().getLocation();
    }

    /**
     * Method to delete a S3 location using the AWS API
     */
    private void purgeS3Partition() {
        //todo
    }

    public static boolean verifyPartitionKey(Table table){
        //edi_business_day='2020-02-20'
        boolean validPartitionKey = false;
        //Test if partition key is "edi_business_day", if so, return true
        if (table.getPartitionKeys().get(0).getName().equals("edi_business_day")){
            validPartitionKey = true;
        }
        return validPartitionKey;
    }

    public static  boolean verifyPartitionKey(String partitionName){
        //edi_business_day='2020-02-20'
        boolean partitionKey = false;
        //Test if partition key is "edi_business_day", if so, return true
        if (partitionName.startsWith("edi_business_day")){
            partitionKey = true;
        }
        return partitionKey;
    }

    public static  String returnPartitionDate(String partitionName){
        String partitionKey = null;
        //Test if partition key is "edi_business_day", if so, return date value
        if (partitionName.startsWith("edi_business_day")){
            partitionKey = partitionName.split("=")[1];
        }
        return partitionKey;
    }

    public static Pattern getTableType(TableMetadata tableMetadata) throws SourceException {
        TableDescriptor tableDescriptor = tableMetadata.tableDescriptor;
        logger.info("Now processing table "+tableDescriptor.getDatabaseName()+"."+tableDescriptor.getTableName());
        Pattern pattern = null;
        if (tableDescriptor.isPartitioned()){
            Partition p = tableDescriptor.getPartitionList().get(0);
            //Test that partition name starts with "edi_business_day" and value matches
            if (verifyPartitionKey(tableDescriptor.getTable()) && p.getValues().size() == 3) {
                pattern = Pattern.EAS;
            } else if (verifyPartitionKey(tableDescriptor.getTable()) && p.getValues().size() == 1){
                pattern = Pattern.SH;
            } else {
                logger.error("Partition specification pattern not recognised");
            }
        } else {
            logger.error("Table "+tableDescriptor.getDatabaseName()+"."+tableDescriptor.getTableName()+" is not partitioned");
        }
        logger.debug("Pattern confirmed as : "+pattern.toString());

        return pattern;
    }


}
