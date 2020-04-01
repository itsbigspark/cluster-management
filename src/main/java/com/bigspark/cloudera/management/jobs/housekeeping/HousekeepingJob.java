package com.bigspark.cloudera.management.jobs.housekeeping;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.common.metadata.HousekeepingMetadata;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Date;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.bigspark.cloudera.management.helpers.FileSystemHelper.getCreateTrashBaseLocation;
import static com.bigspark.cloudera.management.helpers.MetadataHelper.verifyPartitionKey;


/**
 * Housekeeping job
 * Used to purge files from HDFS based on a provided table retention parameter
 *
 * @author Chris Finlayson
 */
class HousekeepingJob {

    public Properties jobProperties;
    public SparkHelper.AuditedSparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public ClusterManagementJob clusterManagementJob;
    public AuditHelper auditHelper;
    public HousekeepingMetadata housekeepingMetadata;
    public SourceDescriptor sourceDescriptor;

    String trashBaseLocation;
    Pattern pattern;
    Dataset<Row> partitionMonthEnds;

    Logger logger = LoggerFactory.getLogger(getClass());

    HousekeepingJob() throws IOException, MetaException, ConfigurationException, SourceException {
        this.clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob,"EDH Cluster housekeeping");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    }

    protected void getTablePartitionMonthEnds(String dbName, String tableName){
        StringBuilder sb = new StringBuilder();
        if (pattern == pattern.EAS){
            sb.append(String.format("SELECT" +
                            " SRC_SYS_ID" +
                            ", SRC_SYS_INST_ID" +
                            ", to_date(year(EDI_BUSINESS_DAY)" +
                            "||'-'||lpad(month(EDI_BUSINESS_DAY),2,'0')" +
                            "||'-'||lpad(MAX(day(EDI_BUSINESS_DAY)),2,'0')" +
                            ") AS MONTH_END" +
                            " FROM %s.%s " +
                            "GROUP BY SRC_SYS_ID, SRC_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
                    , dbName,tableName));
             partitionMonthEnds = spark.sql(sb.toString());
        }
        else if (pattern == pattern.SH){
            sb.append(String.format("SELECT" +
                            " SRC_SYS_INST_ID" +
                            ", to_date(year(EDI_BUSINESS_DAY)" +
                            "||'-'||lpad(month(EDI_BUSINESS_DAY),2,'0')" +
                            "||'-'||lpad(MAX(day(EDI_BUSINESS_DAY)),2,'0')" +
                            ") AS MONTH_END" +
                            " FROM %s.%s " +
                            "GROUP BY SRC_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
                    , dbName,tableName));
            partitionMonthEnds = spark.sql(sb.toString());
        }
        partitionMonthEnds.show();
    }



//    /**
//     * Method to verify if partition key is a month end
//     * @param partition
//     * @return
//     */
//    @Deprecated
//    protected boolean isPartitionKeyMonthEnd(Partition partition) {
//        boolean isPartitionKeyMonthEnd = false;
//        for (Row row : partitionMonthEndsList){
//            if (pattern == Pattern.EAS){
//                String businessDate = partition.getValues().get(0).split("=")[1];
//                String sourceSysId = partition.getValues().get(1).split("=")[1];
//                String sourceSysInstId = partition.getValues().get(2).split("=")[1];
//                if (    row.get(0).equals(sourceSysId) &&
//                        row.get(1).equals(sourceSysInstId) &&
//                        row.get(2).equals(businessDate)){
//                    isPartitionKeyMonthEnd = true;
//                }
//            } else if (pattern == Pattern.SH){
//                String businessDate = partition.getValues().get(0).split("=")[1];
//                // THIS DOESNT WORK AS DOESNT ACCOUNT FOR SRC_SYS_INST_ID
//                if (row.get(1).equals(businessDate)){
//                    isPartitionKeyMonthEnd = true;
//                }
//            }
//        }
//        return isPartitionKeyMonthEnd;
//    }

    /**
     * Method to calculate the minimum retention date (purge ceiling)
     * All dates before that value should be included into purge scope.
     * @param retentionPeriod
     * @param currentDate
     * @return LocalDate
     */
    protected LocalDate calculatePurgeCeiling(int retentionPeriod, LocalDate currentDate) {
        logger.debug("Retention period : "+ retentionPeriod);
        if (retentionPeriod <= 0) {
            logger.debug("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
            return currentDate;
        }
        return currentDate.minusDays(retentionPeriod);
    }

    /**
     * Method to drop table partition using HiveMetaStoreClient
     * @param partition
     * @throws TException
     */
    protected void dropHivePartition(Partition partition) throws TException {
        if (isDryRun){
            logger.info("DRY RUN - Dropped partition : "+partition.toString());
        } else {
            hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues());
            logger.info("Dropped partition : " + partition.toString());
        }
    }

    /**
     * Method to drop table partition and delete data using HiveMetaStoreClient
     * @param partition
     * @throws TException
     */
    protected void purgeHivePartition(Partition partition) throws TException {
        if (isDryRun){
            logger.info("DRY RUN - Purged partition : "+partition.toString());
        } else {
            hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues(), true);
            logger.info("Purged partition : "+partition.toString());
        }
    }

    /**
     * Method to determine if a partition should be included into purge scope or not
     * @param partitionList
     * @param purgeDateCeiling
     * @return
     */
    protected List<Partition> getAllPurgeCandidates(List<Partition> partitionList, LocalDate purgeDateCeiling) {
        ArrayList<Partition> eligiblePartitions = new ArrayList<>();
        partitionList.forEach(
                partition -> {
                    java.util.Date partitionDate = null;
                    try {
                        partitionDate = metadataHelper.getPartitionDate(partition, pattern);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if (partitionDate.before(Date.valueOf(purgeDateCeiling))){
                    eligiblePartitions.add(partition);
                    }
                }
            );
        return eligiblePartitions;
    }

    protected void createMonthEndSwingTable(String database, String table) {
        partitionMonthEnds.createOrReplaceTempView("partitionMonthEnds");
        if (pattern == Pattern.SH) {
            spark.sql(String.format("DROP TABLE IF EXISTS %s.%s_swing",database,table));
            spark.sql(String.format("CREATE TABLE %s.%s_swing AS SELECT t.* from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SRC_SYS_INST_ID = me.SRC_SYS_INST_ID",database,table,database,table));
        } else if (pattern == Pattern.EAS) {
            spark.sql(String.format("DROP TABLE IF EXISTS %s.%s_swing",database,table));
            spark.sql(String.format("CREATE TABLE %s.%s_swing AS SELECT t.* from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SRC_SYS_ID = me.SRC_SYS_ID and t.SRC_SYS_INST_ID = me.SRC_SYS_INST_ID",database,table,database,table));
        }
    }



    protected void trashDataOutwithRetention(List<Partition> purgeCandidates) throws IOException, URISyntaxException {
        for (Partition p : purgeCandidates){
            URI partitionLocation = new URI(p.getSd().getLocation());
            FileSystemHelper.moveDataToUserTrashLocation(partitionLocation.getPath(),trashBaseLocation, isDryRun,fileSystem,sourceDescriptor, auditHelper, logger);
        }
    }

    protected void cleanUpPartitions(List<Partition> purgeCandidates){
        purgeCandidates.forEach(p -> {
            try {
                dropHivePartition(p);
            } catch (TException e) {
                e.printStackTrace();
            }
        });
    }

    protected void resolveSourceTableWithSwingTable(String database, String table){
        Dataset<Row> swingTable = clusterManagementJob.spark.table(String.format("%s.%s_swing", database, table));
        if (pattern==Pattern.SH){
//            swingTable.write().partitionBy("EDI_BUSINESS_DAY").insertInto(String.format("%s.%s", database, table));
            swingTable.write().insertInto(String.format("%s.%s", database, table));
//            insertInto() can't be used together with partitionBy()
//            Partition columns have already been defined for the table.
//            It is not necessary to use partitionBy().
        } else if (pattern==Pattern.EAS){
//               swingTable.write().partitionBy("EDI_BUSINESS_DAY,SOURCE_SYS_ID,SOURCE_SYS_INST_ID").insertInto(String.format("%s.%s", database, table));
               swingTable.write().insertInto(String.format("%s.%s", database, table));
        }
    }

    /**
     * Method to delete a HDFS location using filesystem API
     * @param partition
     * @throws IOException
     */
    protected void purgeHDFSPartition(Partition partition) throws IOException {
        boolean delete = fileSystem.delete(new Path(metadataHelper.getPartitionLocation(partition)), true);
        if (! delete ){
            throw new IOException("Unexpected error deleting location: "+metadataHelper.getPartitionLocation(partition));
        }
    }

    /**
     * Method to execute an Invalidate metadata statement on a table for Impala
     */
    protected void invalidateMetadata() {
        //todo
    }

    protected void getTableType(HousekeepingMetadata housekeepingMetadata) throws SourceException {
        TableDescriptor tableDescriptor = housekeepingMetadata.tableDescriptor;
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
        this.pattern=pattern;
    }

    /**
     * Main entry point method for executing the housekeeping process
     * @throws MetaException
     * @throws SourceException
     */
    void execute(HousekeepingMetadata housekeepingMetadata) throws SourceException, IOException, URISyntaxException {
        this.housekeepingMetadata = housekeepingMetadata;
        this.sourceDescriptor = new SourceDescriptor(metadataHelper.getDatabase(housekeepingMetadata.database), housekeepingMetadata.tableDescriptor);
        this.trashBaseLocation = getCreateTrashBaseLocation("Housekeeping");
        getTableType(housekeepingMetadata);
        if (this.pattern != null){
            LocalDate purgeCeiling = calculatePurgeCeiling(housekeepingMetadata.retentionPeriod, LocalDate.now());
            logger.debug("Purge ceiling calculated as : "+purgeCeiling.toString());
            List<Partition> allPurgeCandidates = getAllPurgeCandidates(housekeepingMetadata.tableDescriptor.getPartitionList(), purgeCeiling);
            logger.debug("Partitions returned as eligible for purge : "+allPurgeCandidates.size());
            if (housekeepingMetadata.isRetainMonthEnd){
                logger.debug("RetainMonthEnd config passed from metadata table");
                getTablePartitionMonthEnds(housekeepingMetadata.database, housekeepingMetadata.tableName);
                    logger.debug("Creating swing table for month end partitions");
                createMonthEndSwingTable(housekeepingMetadata.database, housekeepingMetadata.tableName);
                trashDataOutwithRetention(allPurgeCandidates);
                logger.debug("Resolving source table by reinstating month end partitions");
                resolveSourceTableWithSwingTable(housekeepingMetadata.database, housekeepingMetadata.tableName);
            } else {
                trashDataOutwithRetention(allPurgeCandidates);
            }
        }
    }



}
