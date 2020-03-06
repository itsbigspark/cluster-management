package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HousekeepingController extends ClusterManagementJob {

    public HousekeepingController() throws IOException, MetaException, ConfigurationException {
    }

    Logger logger = LoggerFactory.getLogger(getClass());

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
        return spark.sql("SELECT DISTINCT DATABASE FROM " + getRetentionTable()).collectAsList();
    }

    /**
     * Method to pull distinct list of databases for purging scoping
     *
     * @return List<Row>
     */
    private List<Row> getRetentionGroup(String group) {
        return spark.sql("SELECT DISTINCT DATABASE FROM " + getRetentionTable()+ "WHERE GROUP="+group).collectAsList();
    }


    /**
     * Method to pull list of tables in a specific database for purging
     *
     * @return List<Row>
     */
    private List<Row> getRetentionDataForDatabase(String database) {
        return spark.sql("SELECT TABLE, RETENTION_PERIOD, RETAIN_MONTH_END FROM " + getRetentionTable() + " WHERE DATABASE = '" + database + "' AND ACTIVE='Y'").collectAsList();
    }

    /**
     * Class to store an array of hive metadata for a database
     */
    class HiveMetastoreContainer {
        String databaseName;
        ArrayList<String> allTables;
        ArrayList<TableDescriptor> allTableDescriptors;
    }

    /**
     * Class to store an array of housekeeping retention metadata
     */
    class RetentionMetadataContainer {
        String databaseName;
        ArrayList<TableRetentionMetadata> RetentionMetadataContainerArrayList;

    }

    class TableRetentionMetadata {
        String tableName;
        Integer retentionPeriod;
        boolean isRetainMonthEnd;

        public TableRetentionMetadata(String tableName, Integer retentionPeriod, boolean isRetainMonthEnd) {
            this.tableName = tableName;
            this.retentionPeriod = retentionPeriod;
            this.isRetainMonthEnd = isRetainMonthEnd;
        }
    }

    /**
     * Method to collect all Hive metadata for a specific database
     *
     * @param database
     * @return HiveMetastoreContainer
     * @throws SourceException
     */

    private HiveMetastoreContainer sourceDatabaseFromHiveMetastore(String database) throws SourceException {
        ArrayList<Table> allTablesFromDatabase = metadataHelper.getAllTablesFromDatabase(database);
        ArrayList<TableDescriptor> allTableDescriptors = metadataHelper.getAllTableDescriptors(allTablesFromDatabase);
        HiveMetastoreContainer hiveMetastoreContainer = new HiveMetastoreContainer();
        hiveMetastoreContainer.databaseName = database;
        for (Table table : allTablesFromDatabase)
            hiveMetastoreContainer.allTables.add(table.getTableName());
        hiveMetastoreContainer.allTableDescriptors = allTableDescriptors;
        return hiveMetastoreContainer;
    }

    /**
     * Method to iterate over all databases and return a list of HiveMetastoreContainers
     *
     * @return List<HiveMetastoreContainer>
     * @throws SourceException
     * @throws MetaException
     */
    private List<HiveMetastoreContainer> sourceAllDatabasesFromHiveMetastore() throws SourceException, MetaException {
        List<String> allDatabasesInMetastore = metadataHelper.getAllDatabases();
        List<HiveMetastoreContainer> allHiveMetastoreItems = null;
        for (String database : allDatabasesInMetastore) {
            HiveMetastoreContainer hiveMetastoreContainer = sourceDatabaseFromHiveMetastore(database);
            allHiveMetastoreItems.add(hiveMetastoreContainer);
        }
        return allHiveMetastoreItems;
    }

    /**
     * Method to fetch the housekeeping metadata for a specific database
     *
     * @param database
     * @return RetentionMetadataContainer
     */
    private RetentionMetadataContainer sourceDatabaseFromMetaTable(String database) {
        RetentionMetadataContainer retentionMetadataContainer = new RetentionMetadataContainer();
        retentionMetadataContainer.databaseName = database;
        logger.info("Now collecting purge metadata for database : " + database);
        List<Row> purgeTables = getRetentionDataForDatabase(database);
        logger.info(purgeTables.size() + " tables returned with a purge configuration");
        for (Row table : purgeTables)
            retentionMetadataContainer.RetentionMetadataContainerArrayList.add(
                    new TableRetentionMetadata(
                            table.get(0).toString()
                            ,(Integer) table.get(1)
                            ,Boolean.parseBoolean(String.valueOf(table.get(2)))
                    ));
        return retentionMetadataContainer;
    }

    /**
     * Method to retrieve a specified group of databases held in the metadata and return a list of RetentionMetadataContainers
     * @return List<RetentionMetadataContainer>
     */
    private List<RetentionMetadataContainer> sourceGroupDatabasesFromMetaTable(String executionGroup) {
        List<Row> allDatabasesInMetaTable = getRetentionDataForDatabase(executionGroup);
        List<RetentionMetadataContainer> allPurgeMetadataItems = null;
        for (Row database : allDatabasesInMetaTable) {
            RetentionMetadataContainer RetentionMetadataContainer = sourceDatabaseFromMetaTable(database.get(0).toString());
            allPurgeMetadataItems.add(RetentionMetadataContainer);
        }
        return allPurgeMetadataItems;
    }


    /**
     * Method to iterate over all databases held in the metadata and return a list of RetentionMetadataContainers
     * @return List<RetentionMetadataContainer>
     */
    private List<RetentionMetadataContainer> sourceAllDatabasesFromMetaTable() {
        List<Row> allDatabasesInMetaTable = getRetentionDatabases();
        List<RetentionMetadataContainer> allPurgeMetadataItems = null;
        for (Row database : allDatabasesInMetaTable) {
            RetentionMetadataContainer RetentionMetadataContainer = sourceDatabaseFromMetaTable(database.get(0).toString());
            allPurgeMetadataItems.add(RetentionMetadataContainer);
        }
        return allPurgeMetadataItems;
    }

    void executeHousekeepingGroupForAll() throws ConfigurationException, IOException, MetaException {
        List<RetentionMetadataContainer> allPurgeMetadata = sourceAllDatabasesFromMetaTable();
        HousekeepingJob housekeepingJob = new HousekeepingJob();
        allPurgeMetadata.forEach(database -> {
            try {
                HiveMetastoreContainer hiveMetastoreContainer = sourceDatabaseFromHiveMetastore(database.databaseName);
                hiveMetastoreContainer.allTableDescriptors.forEach(housekeepingJob::execute);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    void executeHousekeepingGroup(String executionGroup) throws ConfigurationException, IOException, MetaException{
        List<RetentionMetadataContainer> allPurgeMetadata = sourceGroupDatabasesFromMetaTable(executionGroup);
        HousekeepingJob housekeepingJob = new HousekeepingJob();
        allPurgeMetadata.forEach(database -> {
            try {
                HiveMetastoreContainer hiveMetastoreContainer = sourceDatabaseFromHiveMetastore(database.databaseName);
                hiveMetastoreContainer.allTableDescriptors.forEach(housekeepingJob::execute);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

    }
}

