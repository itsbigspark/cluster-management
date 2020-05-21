package com.bigspark.cloudera.management.jobs.compaction;

import static com.bigspark.cloudera.management.helpers.FileSystemHelper.getCreateTrashBaseLocation;

import com.bigspark.cloudera.management.common.enums.JobType;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.ClusterManagementTableJob;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Some;

/**
 * @author Chris Finlayson
 * @name Compaction job
 * @purpose Used to discover and compact small parquet files within partitioned tables
 * @JIRA BIG-4
 */
public class CompactionJob extends ClusterManagementTableJob {
  protected static final String cCOMPACTION_JOB_AUDIT_TABLE_NAME ="SYS_CM_COMPACTION_AUDIT";
  Logger logger = LoggerFactory.getLogger(getClass());
  protected SourceDescriptor sourceDescriptor;

  public final CompactionMetadata compactionMetadata;

  protected final String trashBaseLocation;

  public CompactionJob(ClusterManagementJob existing, CompactionMetadata compactionMetadata) throws Exception {
    super(existing, compactionMetadata.database, compactionMetadata.tableName, CompactionJob.cCOMPACTION_JOB_AUDIT_TABLE_NAME);
    this.compactionMetadata = compactionMetadata;
    this.trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("compact");
  }

  public CompactionJob(CompactionMetadata compactionMetadata) throws Exception {
    super(compactionMetadata.database, compactionMetadata.tableName, CompactionJob.cCOMPACTION_JOB_AUDIT_TABLE_NAME);
    this.compactionMetadata = compactionMetadata;
    this.trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("compact");
  }

  /**
   * Method to get currently configured blocksize from Hadoop configuration
   *
   * @return
   */
  protected long getBlocksize() {
    return Long.parseLong(spark.sparkContext().hadoopConfiguration().get("dfs.blocksize"));
  }



  /**
   * Method to pull partition list of supplied database table from Hive metastore via Catalyst
   *
   * @param dbName
   * @param tableName
   * @return Row[]
   */
  protected List<Partition> getTablePartitions(String dbName, String tableName) throws SourceException {
    return metadataHelper.getTablePartitions(dbName, tableName);
  }



  /**
   * Method to return object count and total size of contents for a given HDFS location
   *
   * @param location
   * @return long[]
   * @throws IOException
   */
  protected long[] getFileCountTotalSize(String location) throws IOException {
    ContentSummary cs = fileSystem.getContentSummary(new Path(location));
    long[] returnArray;
    returnArray = new long[2];
    returnArray[0] = cs.getFileCount();
    returnArray[1] = cs.getLength();
    return returnArray;
  }

  /**
   * Method to determine if compaction is required or not Criteria 1 - Average file size must be
   * less than 50% of the configured blocksize 2 - There must be more than 2 files in the directory
   * (1 + accounting for any _SUCCESS notify files etc)
   *
   * @param numFiles
   * @param totalSize
   * @return Boolean
   */
  protected Boolean isCompactionCandidate(Long numFiles, Long totalSize) {
    if (numFiles <= 1 || totalSize == 0) {
      return false;
    }
    long averageSize = totalSize / numFiles;
    double blocksizethreshold = Double.parseDouble(
        jobProperties
            .getProperty("compaction.blocksizethreshold")
    );

    //Check if average file size < parameterise % threshold of blocksize
    //Only compact if there is a small file issue
    return (averageSize < getBlocksize() * blocksizethreshold) && numFiles > 2;
  }

  /**
   * Method to calculate the factor that we should repartition to rounded up integer - total size of
   * location / blocksize
   *
   * @param totalSize
   * @return Integer
   */
  protected Integer getRepartitionFactor(Long totalSize) {
    Long bs = getBlocksize();
    float num = (float) totalSize / bs;
    Integer factor = (int) Math.ceil(num);
    return factor;
  }

  /**
   * Method to execute all required actions against a table partition
   *
   * @param partition
   * @throws IOException
   */
  protected void processPartition(Partition partition)
      throws IOException, TException, URISyntaxException {
    //logger.info("Now processing partition : " + database + "." + table + " [ " + partition.toString() + " ]");
    //String absLocation = partition.getSd().getLocation();
    URI partitionLocation = new URI(partition.getSd().getLocation());
    processLocation(partitionLocation.getPath());
    partition.getParameters().put("compacted", "true");
    hiveMetaStoreClient.alter_partition(partition.getDbName(),partition.getTableName(),partition);
  }

  protected void processLocation(String absLocation) throws IOException {
    long[] countSize = getFileCountTotalSize(absLocation);
    logger.info("Original file count : " + countSize[0]);
    if (isCompactionCandidate(countSize[0], countSize[1])) {
      Integer repartitionFactor = getRepartitionFactor(countSize[1]);
      compactLocation(absLocation, repartitionFactor);
      long[] newCountSize = getFileCountTotalSize(absLocation + "_tmp");
      logger.info("Resulting file count : " + newCountSize[0]);
      resolveLocation(absLocation);
    } else {
      logger.debug("No compaction required for location : [ " + absLocation + " ]");
    }
  }

  /**
   * Method to execute the compaction of a HDFS location
   *
   * @param location
   * @param repartitionFactor
   */
  protected void compactLocation(String location, Integer repartitionFactor) {
    Dataset src = spark.read().parquet(location);
    src.repartition(repartitionFactor)
        .write()
        .mode("overwrite")
        .parquet(location + "_tmp");
    Dataset tmp = spark.read().parquet(location + "_tmp");
    reconcileOutput(src, tmp);

  }

  /**
   * Method to  reconcile newly compacted data against the original partition data except
   * operation(same as MINUS set operation) between two locations
   *
   * @param src
   * @param tmp
   */
  protected void reconcileOutput(Dataset src, Dataset tmp) {
    if (src.except(tmp).count() > 0) {
      logger.error("FATAL: Compacted file has not reconciled to source location");
      logger.error("FATAL: Exiting abnormally");
      System.exit(3);
    }
  }


  protected void moveDataToTrash(String trashBaseLocation, String itemLocation,
      SourceDescriptor sourceDescriptor) throws IOException, URISyntaxException {
    FileSystemHelper
        .moveDataToUserTrashLocation(itemLocation, trashBaseLocation, isDryRun, fileSystem);
    String payload = this.getJobAuditLogRecord(
        itemLocation
        , trashBaseLocation);
    this.JobPartitionAudit.write(payload);
  }


  /**
   * Method to complete the HDFS actions required to swap the compacted data with the original
   *
   * @param location
   * @throws IOException
   */
  protected void resolveLocation(String location) throws IOException {
    String trashLocation = null;
    //Move original data to trash
    try {
      this.moveDataToTrash(trashBaseLocation, location, sourceDescriptor);
    } catch (Exception e) {
      logger.error("FATAL: Unable to move uncompacted files from : " + location
          + " to Trash location");
      e.printStackTrace();
      System.exit(1);
    }
    //Move _tmp location to oriinal localtion
    try {
      boolean rename = fileSystem
          .rename(new Path(location + "_tmp"), new Path(location));
      if (rename) {
      } else {
        throw new IOException("Failed to move files");
      }
    } catch (Exception e) {
      logger.error("ERROR: Unable to move files from temp : " + location
          + "_tmp to partition location");
      e.printStackTrace();

      //Not sure what this does
      try {
        trashLocation = trashBaseLocation + location;
        boolean rename = fileSystem.rename(new Path(trashLocation), new Path(location));
        if (rename) {
        } else {
          throw new IOException("Failed to move files");
        }
      } catch (Exception e_) {
        logger.error("FATAL: Error while reinstating location at " + location);
        logger.error("FATAL: !!!  Locaion is now impacted, resolution required  !!!!");
        e_.printStackTrace();
        System.exit(1);
      }
    }
  }

  /**
   * Main method to execute the compaction process, called by the Runner class
   *
   * @throws IOException
   * @throws NoSuchTableException
   * @throws NoSuchDatabaseException
   */
  void execute()
      throws IOException, SourceException, TException {
    if (this.compactionMetadata.constructor == 1) {
      this.jobAuditBegin();
      logger.info("Now processing table : " + this.databaseName + "." + tableName);
      org.apache.hadoop.hive.metastore.api.Table tableMeta = this.metadataHelper
          .getTable(this.databaseName, this.tableName);
      TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
      this.sourceDescriptor = new SourceDescriptor(metadataHelper.getDatabase(this.databaseName),
          tableDescriptor);
      List<Partition> partitions = getTablePartitions(this.databaseName, this.tableName);
      logger.info("Partitions returned for checking : " + partitions.size());
      ArrayList<Partition> eligiblePartitions = this.removeCompactedPartitions(partitions);

      for (Partition partition : eligiblePartitions) {
        try {
          processPartition(partition);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      this.jobAuditEnd();
    }
    if (this.compactionMetadata.constructor == 2)
      processLocation(this.compactionMetadata.path.toString());
  }

  @Override
  public JobType getJobType() {
    return JobType.COMPACT;
  }


  public ArrayList<Partition>  removeCompactedPartitions(List<Partition> partitions ) {
    ArrayList<Partition> eligiblePartitions = new ArrayList<>();
    partitions.forEach( partition -> {
      if (this.metadataHelper.isCompacted(partition)) {
        logger.debug("Partition already previously compacted : "+ partition.toString());
      } else {
        logger.trace("Adding partition : "+partition.toString());
        eligiblePartitions.add(partition);
      }
    });
    return eligiblePartitions;
  }

}

