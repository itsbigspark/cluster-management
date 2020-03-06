package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Housekeeping job
 * Used to purge files from HDFS or S3 depending on table retention parameter
 *
 * @author Chris Finlayson
 */
class HousekeepingJob extends ClusterManagementJob {

    Logger logger = LoggerFactory.getLogger(getClass());
    String trashBaseLocation;
    boolean dryRun;
    Pattern pattern;
    Dataset<Row> partitionMonthEnds;
    private List<Row> partitionMonthEndsList;


    HousekeepingJob() throws IOException, MetaException, ConfigurationException {
        super();
    }

    private void getTablePartitionMonthEnds(String dbName, String tableName){
        StringBuilder sb = new StringBuilder();
        if (pattern == pattern.EAS){
            sb.append(String.format
                    ("SELECT DISTINCT" +
                    "SOURCE_SYS_ID" +
                    ", SOURCE_SYS_INST_ID" +
                    ", year(EDI_BUSINESS_DAY)" +
                    "||'-'||month(EDI_BUSINESS_DAY)" +
                    "||'-'||max(day(EDI_BUSINESS_DAY)) AS MONTH_END" +
                    " FROM %s.%s " +
                    "GROUP BY SOURCE_SYS_ID, SOURCE_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
                    , dbName, tableName));
             partitionMonthEnds = spark.sql(sb.toString());
             partitionMonthEndsList = spark.sql(sb.toString()).collectAsList();
        }
        else if (pattern == pattern.SH){
            sb.append(String.format("SELECT DISTINCT" +
                            ", SOURCE_SYS_INST_ID" +
                            ", year(EDI_BUSINESS_DAY)" +
                            "||'-'||month(EDI_BUSINESS_DAY)" +
                            "||'-'||max(day(EDI_BUSINESS_DAY)) AS MONTH_END" +
                            " FROM %s.%s " +
                            "GROUP BY SOURCE_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
                    , dbName,tableName));
            partitionMonthEnds = spark.sql(sb.toString());
            partitionMonthEndsList = spark.sql(sb.toString()).collectAsList();
        }
    }



    /**
     * Method to verify if partition key is a month end
     * @param partition
     * @return
     */
    @Deprecated
    private boolean isPartitionKeyMonthEnd(Partition partition) {
        boolean isPartitionKeyMonthEnd = false;
        for (Row row : partitionMonthEndsList){
            if (pattern == Pattern.EAS){
                String businessDate = partition.getValues().get(0).split("=")[1];
                String sourceSysId = partition.getValues().get(1).split("=")[1];
                String sourceSysInstId = partition.getValues().get(2).split("=")[1];
                if (    row.get(0).equals(sourceSysId) &&
                        row.get(1).equals(sourceSysInstId) &&
                        row.get(2).equals(businessDate)){
                    isPartitionKeyMonthEnd = true;
                }
            } else if (pattern == Pattern.SH){
                String businessDate = partition.getValues().get(0).split("=")[1];
                // THIS DOESNT WORK AS DOESNT ACCOUNT FOR SRC_SYS_INST_ID
                if (row.get(1).equals(businessDate)){
                    isPartitionKeyMonthEnd = true;
                }
            }
        }
        return isPartitionKeyMonthEnd;
    }

    /**
     * Method to calculate the minimum retention date (purge ceiling)
     * All dates before that value should be included into purge scope.
     * @param retentionPeriod
     * @param currentDate
     * @return LocalDate
     */
    private LocalDate calculatePurgeCeiling(int retentionPeriod, LocalDate currentDate) {
        if (retentionPeriod <= 0) {
            logger.debug("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
        }
        return currentDate.minusDays(retentionPeriod);
    }

    /**
     * Method to drop table partition using HiveMetaStoreClient
     * @param partition
     * @throws TException
     */
    private void dropHivePartition(Partition partition) throws TException {
        hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues());
    }

    /**
     * Method to drop table partition and delete data using HiveMetaStoreClient
     * @param partition
     * @throws TException
     */
    private void purgeHivePartition(Partition partition) throws TException {
        hiveMetaStoreClient.dropPartition(partition.getDbName(), partition.getTableName(), partition.getValues(), true);
    }

    /**
     * Method to determine if a partition should be included into purge scope or not
     * @param partitionList
     * @param purgeDateCeiling
     * @return
     */
    private List<Partition> getAllPurgeCandidates(List<Partition> partitionList, LocalDate purgeDateCeiling, boolean retainMonthEnd) {
        List<Partition> eligiblePartitions = partitionList.stream()
                //filter all partitions that are month end if this is set
//                .filter(partition -> {
//                    if (isPartitionKeyMonthEnd(partition) && retainMonthEnd) {
//                        logger.error("Partition is a month end [ " + partition.getValues().toString() + " ] and month ends are to be retained for this table. ");
//                        return false;
//                    } else {
//                        return true;
//                    }
//                })
                //find partitions older than purgeCeiling
                .filter(partition -> {
                    if (metadataHelper.getPartitionDate(partition).before(Date.valueOf(purgeDateCeiling))) {
                        return true;
                    } else {
                        logger.debug(partition.toString() + "is within retention period [ Purge ceiling = " + purgeDateCeiling + " ]");
                        return false;
                    }
                })
                .collect(Collectors.toList());

        return eligiblePartitions;
    }

    private void createMonthEndSwingTable(String database, String table) {
        partitionMonthEnds.createOrReplaceTempView("partitionMonthEnds");
        if (pattern == Pattern.SH) {
            spark.sql(String.format("CREATE TABLE %s.%s_hkp AS SELECT * from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SOURCE_SYS_INST_ID = me.SOURCE_SYS_INST_ID",database,table));
        } else if (pattern == Pattern.EAS) {
            spark.sql(String.format("CREATE TABLE %s.%s_hkp AS SELECT * from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SOURCE_SYS_ID = me.SOURCE_SYS_ID and t.SOURCE_SYS_INST_ID = me.SOURCE_SYS_INST_ID",database,table));
        }
    }

    private void setTrashBaseLocation() throws IOException {
        StringBuilder sb = new StringBuilder();
        String userHomeArea = FileSystemHelper.getUserHomeArea();
        sb.append(userHomeArea).append("/.ClusterManagementTrash/housekeeping");
        trashBaseLocation = sb.toString();
        if (! fileSystem.exists(new Path(trashBaseLocation))){
            fileSystem.mkdirs(new Path(trashBaseLocation));
        }
    }

    private void trashDataOutwithRetention(List<Partition> purgeCandidates) throws IOException {
        for (Partition p : purgeCandidates){ ;
            String trashTarget = trashBaseLocation+"/"+p.getSd().getLocation();
            boolean isRenameSuccess = fileSystem.rename(new Path(p.getSd().getLocation()), new Path(trashTarget));
            if (! isRenameSuccess ){
                throw new IOException(String.format("Failed to move files from : %s to : %s",p.getSd().getLocation(),trashTarget));
            }
        }
    }

    private void cleanUpPartitions(List<Partition> purgeCandidates){
        purgeCandidates.forEach(p -> {
            try {
                dropHivePartition(p);
            } catch (TException e) {
                e.printStackTrace();
            }
        });
    }

    private void resolveSourceTableWithSwingTable(String database, String table){
        Dataset<Row> swingTable = spark.table(String.format("%s.%s_hkp", database, table));
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
    private void purgeHDFSPartition(Partition partition) throws IOException {
        fileSystem.delete(new Path(getPartitionLocation(partition)), true);
    }

    /**
     * Method to return the location defined on a Hive table partition
     * @param partition
     * @return
     */
    private String getPartitionLocation(Partition partition) {
        return partition.getSd().getLocation();
    }

    /**
     * Method to return the location defined on a Hive table
     * @param table
     * @return
     */
    private String getTableLocation(Table table) {
        return table.getSd().getLocation();
    }

    /**
     * Method to delete a S3 location using the AWS API
     */
    private void purgeS3Partition() {
        //todo
    }

    /**
     * Method to execute an Invalidate metadata statement on a table for Impala
     */
    private void invalidateMetadata() {
        //todo
    }

    private void getTableType(TableDescriptor tableDescriptor) throws SourceException {
        Pattern pattern = null;
        if (tableDescriptor.isPartitioned()){
            Partition p = tableDescriptor.getPartitionList().get(0);
            if (p.getValues().size() == 3 && p.getValues().get(0).contains("edi_business_day")) {
                pattern = Pattern.EAS;
            } else if (p.getValues().size() == 1 && p.getValues().get(0).contains("edi_business_day")){
                pattern =  Pattern.SH;
            } else {
                logger.error("Partition specification not recognised");
            }
        }
        this.pattern=pattern;
    }

    /**
     * Main entry point method for executing the housekeeping process
     * @throws MetaException
     * @throws SourceException
     */
    void execute(TableDescriptor tableDescriptor){


    }

}
