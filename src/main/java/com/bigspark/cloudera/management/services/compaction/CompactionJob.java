package com.bigspark.cloudera.management.services.compaction;

import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;

import java.io.IOException;

/**
 * Compaction job class
 * @author Chris Finlayson
 *
 */
public class CompactionJob {

    Logger log = LoggerFactory.getLogger(getClass());
    SparkSession spark;
    FileSystem fs;

    /**
     * Method to get currently configured blocksize from Hadoop configuration
     * @return
     */
    private long getBlocksize(){
        return Long.parseLong(spark.sparkContext().hadoopConfiguration().get("dfs.blocksize"));
    }

    /**
     * Method to extract database listing from Hive metastore via catalog
     * @return Dataset<Database>
     */
    private Dataset<Database> getDatabases(){
        return spark.catalog().listDatabases();
    }

    /**
     * Method to extract table listing of  supplied database from Hive metastore via catalog
     * @param dbName
     * @return Dataset<Table>
     * @throws AnalysisException
     */
    private Dataset<Table> getTables(String dbName) throws AnalysisException {
        return spark.catalog().listTables(dbName);
    }

    /**
     * Method to pull partition list of supplied database table from Hive metastore via Catalyst
     * @param dbName
     * @param tableName
     * @return Row[]
     */
    private Row[] getTablePartitions(String dbName, String tableName){
            return (Row[]) spark.sql("SHOW PARTITIONS " + dbName + "." + tableName).collect();
    }

    /**
     * Method to pull table location of supplied database table from Hive metastore via catalog
     * @param dbName
     * @param tableName
     * @return String
     * @throws NoSuchDatabaseException
     * @throws NoSuchTableException
     */
    private String getTableLocation(String dbName, String tableName) throws NoSuchDatabaseException, NoSuchTableException {
        Some<String> schema = new Some<String>(dbName);
        TableIdentifier tableIdentifier = new TableIdentifier(tableName, schema);
        return spark.sessionState().catalog().getTableMetadata(tableIdentifier).location().getRawPath();
    }

    /**
     * Method to return object count and total size of contents for a given HDFS location i
     * @param location
     * @return Pair<Long,Long>
     * @throws IOException
     */
    private Pair<Long, Long> getFileCountTotalSizePair(String location) throws IOException {
        ContentSummary cs = this.fs.getContentSummary(new Path(location));
         ImmutablePair<Long, Long> pair = new ImmutablePair<>(cs.getFileCount(),cs.getLength());
         return pair;
    }

    /**
     *Method to return object count and total size of contents for a given HDFS location i
     * @param location
     * @return long[]
     * @throws IOException
     */
    private long[] getFileCountTotalSize(String location) throws IOException {
        ContentSummary cs = this.fs.getContentSummary(new Path(location));
        long returnArray [];
        returnArray = new long[2];
        returnArray[0]=cs.getFileCount();
        returnArray[1]=cs.getLength();
        return returnArray;
    }

    /**
     * Method to detemrine if compaction is required or not
     * Criteria
     * 1 - Average file size must be less than 50% of the configured blocksize
     * 2 - There must be more than 2 files in the directory (1 + accounting for any _SUCCESS notify files etc)
     * @param numFIles
     * @param totalSize
     * @return Boolean
     */
    private Boolean isCompactionCandidate(Long numFIles, Long totalSize) {
        if (numFIles == 0 || totalSize == 0) {
            return false;
        }
            Long averageSize = totalSize / numFIles;
            //Check if average file size < 50% of blocksize
            //Only compact if there is a small file issue
            return (averageSize < getBlocksize() * 0.5) && numFIles > 2;
        }

    /**
     * Method to calauclate the factor that we should repartition to
     * rounded up integer - total size of location / blocksize
     * @param totalSize
     * @return Integer
     */
    private Integer getRepartitionFactor(Long totalSize){
        Long bs = getBlocksize();
        float num = (float) totalSize/bs;
        Integer factor = (int)Math.ceil(num);
        return factor;
    }

    /**
     * Method to execute all required actions against a table
     * Iterating over each table partition and compacting
     * @param dbName
     * @param tableName
     * @throws NoSuchDatabaseException
     * @throws NoSuchTableException
     * @throws IOException
     */
    private void processTable(String dbName, String tableName) throws NoSuchDatabaseException, NoSuchTableException, IOException{
       String tableLocation = getTableLocation(dbName,tableName);
       Row[] partitions = getTablePartitions(dbName, tableName);
       for (Row row : partitions){
           processPartition(tableLocation, row.get(0).toString());
       }
    }

    /**
     * Method to execute all required actions against a table partition
     * @param tableLocation
     * @param partition
     * @throws IOException
     */
    private void processPartition(String tableLocation, String partition) throws IOException{
        String absPartitionLocation=tableLocation+"/"+partition;
        long[] countSize = getFileCountTotalSize(absPartitionLocation);
        if (isCompactionCandidate(countSize[0],countSize[1])){
            Integer repartitionFactor = getRepartitionFactor(countSize[1]);
            compactLocation(absPartitionLocation,repartitionFactor);
            long[] newCountSize = getFileCountTotalSize(absPartitionLocation+"_tmp");
//            resolvePartition(absPartitionLocation);
        }
    }

    /**
     * Method to execute the compaction of a HDFS location
     * @param location
     * @param repartitionFactor
     */
    private void compactLocation(String location, Integer repartitionFactor) {
        Dataset src = spark.read().parquet(location);
        src.repartition(repartitionFactor)
                .write()
                .mode("overwrite")
                .parquet(location + "_tmp");
        Dataset tmp = spark.read().parquet(location+"_tmp");
        reconcileOutput(src,tmp);

    }

    /**
     * Method to  reconcile newly compacted data against the original partition data
     * except operation(same as MINUS set operation) between two locations
     * @param src
     * @param tmp
     */
    private void reconcileOutput(Dataset src, Dataset tmp){
        if (src.except(tmp).count()>0){
            log.error("FATAL: Compacted file has not reconciled to source location");
            log.error("FATAL: Exiting abnormally");
            System.exit(3);
        }
    }

    /**
     * Method to complete the HDFS actions required to swap the compacted data with the original
     * @param partitionLocation
     * @throws IOException
     */
    private void resolvePartition(String partitionLocation) throws IOException{
        try {
            fs.rename(new Path(partitionLocation), new Path(partitionLocation + "_delete"));
        } catch (Exception  e){
            System.out.println("ERROR: Unable to move uncompacted files from : "+partitionLocation +" to delete location");
            e.printStackTrace();
        }
        try {
            fs.rename(new Path(partitionLocation+"_tmp"), new Path(partitionLocation));
        } catch (Exception  e){
            System.out.println("ERROR: Unable to move files from temp : "+partitionLocation+"_tmp to partition location" );
            e.printStackTrace();
            // If this happens, we need to try and resolve, oherwise the partition is impacted
            try {
                fs.rename(new Path(partitionLocation+"_delete"),new Path(partitionLocation));
            } catch (Exception e_){
                log.error("FATAL: Error while reinstating partition at "+partitionLocation);
                log.error("FATAL: Partition is now inoperable");
                e_.printStackTrace();
                System.exit(1);
            }
        }
    }

    /**
     * Main method to execute the compaction process, called by the Runner class
     * @throws IOException
     * @throws NoSuchTableException
     * @throws NoSuchDatabaseException
     */
    void executeMist(String database, String table, SparkSession spark) throws IOException, NoSuchTableException,NoSuchDatabaseException{
        this.spark = SparkHelper.getSparkSession();
        this.fs = FileSystemHelper.getConnection();
        processTable(database,table);
    }
    /**
     * Main method to execute the compaction process, called by the Runner class
     * @throws IOException
     * @throws NoSuchTableException
     * @throws NoSuchDatabaseException
     */
    void execute() throws IOException, NoSuchTableException,NoSuchDatabaseException{
        this.spark = SparkHelper.getSparkSession();
        this.fs = FileSystemHelper.getConnection();
        processTable("bddlsold01p","cf_small_files");
    }
}

