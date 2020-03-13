package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HousekeepingController {

    public Properties jobProperties;
    public SparkHelper.AuditedSparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;
    public AuditHelper auditHelper;
    public Boolean isDryRun;

    Logger logger = LoggerFactory.getLogger(getClass());

    public HousekeepingController() throws IOException, MetaException, ConfigurationException, SourceException {
        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob);
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    }

    /**
     * Method to fetch metadata table value from properties file
     *
     * @return String
     */
    private String getRetentionTable() {
        return jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.metatable");
    }

    /**
     * Method to pull distinct list of databases for purging scoping
     *
     * @return List<Row>
     */
    private List<Row> getRetentionDatabases() {
        return spark.sql("SELECT DISTINCT DATABASE FROM " + getRetentionTable()+ " WHERE ACTIVE='true'").collectAsList() ;
    }

    /**
     * Method to pull distinct list of databases in execution group
     *
     * @return List<Row>
     */
    private List<Row> getRetentionGroupDatabases(int group) {
        logger.info("Now pulling list of databases for group : "+ group);
        return spark.sql("SELECT DISTINCT DATABASE FROM " + getRetentionTable()+ " WHERE ACTIVE='true' and GROUP="+group).collectAsList();
    }


    /**
     * Method to pull list of tables in a specific database for purging
     *
     * @return List<Row>
     */
    private List<Row> getRetentionDataForDatabase(String database, int group) {
        logger.info("Now pulling configuration metadata for all tables in database : "+ database);
        return spark.sql("SELECT TABLE, RETENTION_PERIOD, RETAIN_MONTH_END FROM " + getRetentionTable() + " WHERE DATABASE = '" + database + "' AND ACTIVE='true' AND GROUP ="+group).collectAsList();
    }


    class TableMetadata {
        String database;
        String tableName;
        Integer retentionPeriod;
        boolean isRetainMonthEnd;
        TableDescriptor tableDescriptor;

        public TableMetadata(String database, String tableName, Integer retentionPeriod, boolean isRetainMonthEnd, TableDescriptor tableDescriptor) {
            this.database = database;
            this.tableName = tableName;
            this.retentionPeriod = retentionPeriod;
            this.isRetainMonthEnd = isRetainMonthEnd;
            this.tableDescriptor = tableDescriptor;
        }
    }


    /**
     * Method to fetch the housekeeping metadata for a specific database
     *
     * @param database
     * @return RetentionMetadataContainer
     */
    private ArrayList<TableMetadata> sourceDatabaseTablesFromMetaTable(String database, int group) throws SourceException {
        List<Row> purgeTables = getRetentionDataForDatabase(database, group);
        ArrayList<TableMetadata> tableMetadataList = new ArrayList<>();
        logger.info(purgeTables.size() + " tables returned with a purge configuration");
        for (Row table : purgeTables){
            String tableName = table.get(0).toString();
            Integer retentionPeriod = (Integer) table.get(1);
            boolean isRetainMonthEnd = Boolean.parseBoolean(String.valueOf(table.get(2)));
            try{
                Table tableMeta = metadataHelper.getTable(database,tableName);
                TableDescriptor tableDescriptor =  metadataHelper.getTableDescriptor(tableMeta);
                TableMetadata tableMetadata = new TableMetadata(database,tableName,retentionPeriod,isRetainMonthEnd,tableDescriptor);
                tableMetadataList.add(tableMetadata);
            } catch (SourceException e){
                logger.error(tableName+" : provided in metadata configuration, but not found in database..");
            }
        }
        return tableMetadataList;
    }


    public void executeHousekeepingGroup(int executionGroup) throws ConfigurationException, IOException, MetaException, SourceException {
        HousekeepingJob housekeepingJob = new HousekeepingJob();
        auditHelper.startup();
        List<Row> retentionGroup = getRetentionGroupDatabases(executionGroup);
        retentionGroup.forEach(retentionRecord -> {
            String database=retentionRecord.get(0).toString();
            ArrayList<TableMetadata> tableMetadataList = new ArrayList<>();
            try {
                tableMetadataList.addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
            } catch (SourceException e) {
                e.printStackTrace();
            }
            tableMetadataList.forEach(table ->{
                try {
                    housekeepingJob.execute(table);
                } catch (SourceException | IOException e) {
                    e.printStackTrace();
                }
            });
        });
        auditHelper.completion();
    }
}

