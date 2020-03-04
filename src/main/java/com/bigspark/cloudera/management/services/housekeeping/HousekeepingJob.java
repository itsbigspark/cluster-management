package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Housekeeping job
 * Used to purge files from HDFS or S3 depending on table retention parameter
 *
 * @author Chris Finlayson
 */
class HousekeepingJob extends ClusterManagementJob {

    Logger log = LoggerFactory.getLogger(getClass());
    boolean dryRun;

    HousekeepingJob() throws IOException, MetaException, ConfigurationException {
        super();
    }

    private String getRetentionTable() {
        return jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.metatable");
    }

    private List<Row> getRetentionDatabases() {
        return spark.sql("SELECT DISTINCT DATABASE FROM " + getRetentionTable()).collectAsList();
    }

    private List<Row> getRetentionDataForDatabase(String database) {
        return spark.sql("SELECT TABLE, RETENTION FROM " + getRetentionTable() + " WHERE DATABASE = '" + database + "' AND ACTIVE='Y'").collectAsList();
    }

    private boolean isFirstPartitionKeyIsDate(Partition p) {
        //todo
        return true;
    }

    private LocalDate calculatePurgeCeiling(int retentionPeriod, LocalDate currentDate) {
        if (retentionPeriod <= 0) {
            log.debug("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
        }
        return currentDate.minusDays(retentionPeriod);
    }

    private void dropHivePartition(Partition partition) throws TException {
        hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues());
    }

    private void purgeHivePartition(Partition partition) throws TException {
        hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues(), true);
    }

    private List<Partition> getAllPurgeCandidates(List<Partition> partitionList, LocalDate purgeDateCeiling) {
        List<Partition> eligiblePartitions = partitionList.stream()
                .filter(p -> {
                    if (isFirstPartitionKeyIsDate(p)) {
                        return true;
                    } else {
                        log.error("First key of the partition [ " + p.getValues().get(0) + " ] is not a valid date. ");
                        return false;
                    }
                })
                //find partitions older than purgeCeiling
                .filter(partition -> {
                    if (metadataHelper.getPartitionDate(partition).before(Date.valueOf(purgeDateCeiling))) {
                        return true;
                    } else {
                        log.debug(partition.toString() + "is under retention period [ Purge ceiling = " + purgeDateCeiling + " ]");
                        return false;
                    }
                })
                .collect(Collectors.toList());

        return eligiblePartitions;
    }

    private void purgeHDFSPartition(Partition partition) throws IOException {
        fileSystem.delete(new Path(getTableLocation(partition)), true);
    }

    private String getTableLocation(Partition partition) {
        return partition.getSd().getLocation();
    }

    private void purgeS3Partition() {
        //todo
    }

    private void invalidateMetadata() {
        //todo
    }

    private static class HiveMetastoreContainer {
        String databaseName;
        ArrayList<String> allTables;
        ArrayList<TableDescriptor> allTableDescriptors;
    }

    private static class PurgeMetadataContainer {
        String databaseName;
        Map<String,Integer> allTables;
    }

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

    private List<HiveMetastoreContainer> sourceAllDatabasesFromHiveMetastore() throws SourceException, MetaException {
        List<String> allDatabasesInMetastore = metadataHelper.getAllDatabases();
        List<HiveMetastoreContainer> allHiveMetastoreItems = null;
        for (String database : allDatabasesInMetastore) {
            HiveMetastoreContainer hiveMetastoreContainer = sourceDatabaseFromHiveMetastore(database);
            allHiveMetastoreItems.add(hiveMetastoreContainer);
        }
        return allHiveMetastoreItems;
    }

    private PurgeMetadataContainer sourceDatabaseFromMetaTable(String database){
        PurgeMetadataContainer purgeMetadataContainer = new PurgeMetadataContainer();
        purgeMetadataContainer.databaseName = database;
        log.info("Now collecting purge metadata for database : " + database);
        List<Row> purgeTables = getRetentionDataForDatabase(database);
        log.info(purgeTables.size() + " tables returned with a purge configuration");
        for (Row table : purgeTables)
            purgeMetadataContainer.allTables.put(table.get(0).toString(), (Integer) table.get(1));
        return purgeMetadataContainer;
    }


    private List<PurgeMetadataContainer> sourceAllDatabasesFromMetaTable() {
        List<Row> allDatabasesInMetaTable = getRetentionDatabases();
        List<PurgeMetadataContainer> allPurgeMetadataItems = null;
        for (Row database : allDatabasesInMetaTable) {
            PurgeMetadataContainer purgeMetadataContainer = sourceDatabaseFromMetaTable(database.get(0).toString());
            allPurgeMetadataItems.add(purgeMetadataContainer);
        }
        return allPurgeMetadataItems;
    }

    void execute() throws MetaException, SourceException {
        List<PurgeMetadataContainer> allPurgeMetadata = sourceAllDatabasesFromMetaTable();
        List<Partition> allPurgeCandidates = new ArrayList<>();
        for (PurgeMetadataContainer database : allPurgeMetadata) {
            HiveMetastoreContainer hiveMetastoreContainer = sourceDatabaseFromHiveMetastore(database.databaseName);
            for (TableDescriptor tableDescriptor : hiveMetastoreContainer.allTableDescriptors) {
                LocalDate purgeCeiling = calculatePurgeCeiling(database.allTables.get(tableDescriptor.getTableName()),LocalDate.now()); //todo - To check that now() is acceptable
                List<Partition> purgeCandidates = getAllPurgeCandidates(tableDescriptor.getPartitionList(),purgeCeiling);
                allPurgeCandidates.addAll(purgeCandidates);
            }
        }
    }

}
