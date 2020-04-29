package com.bigspark.cloudera.management.jobs.compaction;

import static com.bigspark.cloudera.management.helpers.FileSystemHelper.getCreateTrashBaseLocation;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper_OLD;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob_OLD;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
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
public class CompactionJob {

  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public String applicationID;
  public String table;
  public String database;
  public AuditHelper_OLD auditHelperOLD;
  protected String trashBaseLocation;

  Logger logger = LoggerFactory.getLogger(getClass());
  protected SourceDescriptor sourceDescriptor;


  public CompactionJob()
      throws IOException, MetaException, ConfigurationException, SourceException {
    ClusterManagementJob_OLD clusterManagementJobOLD = ClusterManagementJob_OLD.getInstance();
    this.auditHelperOLD = new AuditHelper_OLD(clusterManagementJobOLD, "Small file compaction job","compaction.AuditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJobOLD.spark, auditHelperOLD);
    this.fileSystem = clusterManagementJobOLD.fileSystem;
    this.hadoopConfiguration = clusterManagementJobOLD.hadoopConfiguration;
    this.metadataHelper = clusterManagementJobOLD.metadataHelper;
    this.isDryRun = clusterManagementJobOLD.isDryRun;
    this.jobProperties = clusterManagementJobOLD.jobProperties;
    this.hiveMetaStoreClient = clusterManagementJobOLD.hiveMetaStoreClient;
    this.applicationID = SparkHelper.getSparkApplicationId();
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
   * Method to extract database listing from Hive metastore via catalog
   *
   * @return Dataset<Database>
   */
  protected Dataset<Database> getDatabases() {
    return spark.catalog().listDatabases();
  }

  /**
   * Method to extract table listing of  supplied database from Hive metastore via catalog
   *
   * @param dbName
   * @return Dataset<Table>
   * @throws AnalysisException
   */
  protected Dataset<Table> getTables(String dbName) throws AnalysisException {
    return spark.catalog().listTables(dbName);
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
   * Method to pull table location of supplied database table from Hive metastore via catalog
   *
   * @param dbName
   * @param tableName
   * @return String
   * @throws NoSuchDatabaseException
   * @throws NoSuchTableException
   */
  protected String getTableLocation(String dbName, String tableName)
      throws NoSuchDatabaseException, NoSuchTableException {
    Some<String> schema = new Some<String>(dbName);
    TableIdentifier tableIdentifier = new TableIdentifier(tableName, schema);
    return spark.sessionState().catalog().getTableMetadata(tableIdentifier).location().getRawPath();
  }

  /**
   * Method to return object count and total size of contents for a given HDFS location i
   *
   * @param location
   * @return Pair<Long, Long>
   * @throws IOException
   */
  protected Pair<Long, Long> getFileCountTotalSizePair(String location) throws IOException {
    ContentSummary cs = fileSystem.getContentSummary(new Path(location));
    ImmutablePair<Long, Long> pair = new ImmutablePair<>(cs.getFileCount(), cs.getLength());
    return pair;
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
   * Method to execute all required actions against a table Iterating over each table partition and
   * compacting
   *
   * @param dbName
   * @param tableName
   * @throws NoSuchDatabaseException
   * @throws NoSuchTableException
   * @throws IOException
   */
  protected void processTable(String dbName, String tableName)
      throws IOException, SourceException, TException {
    logger.info("Now processing table : " + dbName + "." + tableName);
    org.apache.hadoop.hive.metastore.api.Table tableMeta = metadataHelper
        .getTable(dbName, tableName);
    TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
    this.sourceDescriptor = new SourceDescriptor(metadataHelper.getDatabase(dbName),
        tableDescriptor);
    List<Partition> partitions = getTablePartitions(dbName, tableName);
    logger.info("Partitions returned for checking : " + partitions.size());
    ArrayList<Partition> eligiblePartitions = new ArrayList<>();
    partitions.forEach( partition -> {
      if (partition.getParameters().get("compacted").equals("true")){
        logger.trace("Partition already previously compacted : "+partition.toString());
      } else {
        logger.trace("Adding partition : "+partition.toString());
        eligiblePartitions.add(partition);
      }
    });
    for (Partition partition : eligiblePartitions) {
      try {
        processPartition(partition);
      } catch (IOException | TException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Method to execute all required actions against a table partition
   *
   * @param partition
   * @throws IOException
   */
  protected void processPartition(Partition partition) throws IOException, TException {
    logger.info("Now processing partition : " + database + "." + table + " [ " + partition.toString() + " ]");
    String absLocation = partition.getSd().getLocation();
    processLocation(absLocation);
    partition.getParameters().put("compacted", "true");
    hiveMetaStoreClient.alter_partition(partition.getDbName(),partition.getTableName(),partition);
  }

  protected void processLocation(String absLocation) throws IOException {
    long[] countSize = getFileCountTotalSize(absLocation);
    logger.info("Original file count : " + countSize[0]);
    if (isCompactionCandidate(countSize[0], countSize[1])) {
      auditHelperOLD.writeAuditLine("Compact", sourceDescriptor.toString(),
          "Original file count : " + countSize[0], true);
      Integer repartitionFactor = getRepartitionFactor(countSize[1]);
      compactLocation(absLocation, repartitionFactor);
      long[] newCountSize = getFileCountTotalSize(absLocation + "_tmp");
      logger.info("Resulting file count : " + newCountSize[0]);
      auditHelperOLD.writeAuditLine("Compact", sourceDescriptor.toString(),
          "Resulting file count : " + countSize[0], true);
      resolveLocation(absLocation);
    } else {
      logger.debug(
          "No compaction required for location : [ " + absLocation + " ]");
      auditHelperOLD.writeAuditLine("Compact", sourceDescriptor.toString(),
          "No compaction required : location - "+absLocation+" , file count - " + countSize[0] + " , location size - " + countSize[1], true);
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

  protected void setTrashBaseLocation() throws IOException {
    StringBuilder sb = new StringBuilder();
    String userHomeArea = FileSystemHelper.getUserHomeArea();
    sb.append(userHomeArea).append("/.ClusterManagementTrash/compaction");
    trashBaseLocation = sb.toString();
    if (!fileSystem.exists(new Path(trashBaseLocation))) {
      fileSystem.mkdirs(new Path(trashBaseLocation));
    }
  }

  protected void moveDataToTrash(String trashBaseLocation, String itemLocation,
      SourceDescriptor sourceDescriptor) throws IOException, URISyntaxException {
    FileSystemHelper
        .moveDataToUserTrashLocation(itemLocation, trashBaseLocation, isDryRun, fileSystem);
  }


  /**
   * Method to complete the HDFS actions required to swap the compacted data with the original
   *
   * @param location
   * @throws IOException
   */
  protected void resolveLocation(String location) throws IOException {
    String trashLocation = null;
    try {
      moveDataToTrash(trashBaseLocation, location, sourceDescriptor);
      auditHelperOLD.writeAuditLine("Trash", sourceDescriptor.toString(),
          String.format("Moved files from : %s to : %s", location, trashBaseLocation),
          true);
    } catch (Exception e) {
      logger.error("FATAL: Unable to move uncompacted files from : " + location
          + " to Trash location");
      e.printStackTrace();
      System.exit(1);
    }
    try {
      boolean rename = fileSystem
          .rename(new Path(location + "_tmp"), new Path(location));
      if (rename) {
        auditHelperOLD.writeAuditLine("Move", sourceDescriptor.toString(),
            String.format("Moved : %s ==> %s", location + "_tmp", location),
            true);
      } else {
        throw new IOException("Failed to move files");
      }
    } catch (Exception e) {
      logger.error("ERROR: Unable to move files from temp : " + location
          + "_tmp to partition location");
      auditHelperOLD.writeAuditLine("Move", sourceDescriptor.toString(),
          String.format("Moving : %s ==> %s", location + "_tmp", location),
          false);
      e.printStackTrace();
      // If this happens, we need to try and resolve, otherwise the partition is impacted
      try {
        trashLocation = trashBaseLocation + location;
        boolean rename = fileSystem.rename(new Path(trashLocation), new Path(location));
        if (rename) {
          auditHelperOLD.writeAuditLine("Move", sourceDescriptor.toString(),
              String.format("Moving : %s ==> %s", trashLocation, location), true);
        } else {
          throw new IOException("Failed to move files");
        }
      } catch (Exception e_) {
        auditHelperOLD.writeAuditLine("Move", sourceDescriptor.toString(),
            String.format("Moving : %s ==> %s", trashLocation, location), false);
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
  void executeMist(String database, String table, SparkSession spark)
      throws IOException,SourceException, TException {
    processTable(database, table);
  }

  /**
   * Main method to execute the compaction process, called by the Runner class
   *
   * @throws IOException
   * @throws NoSuchTableException
   * @throws NoSuchDatabaseException
   */
  void execute(CompactionMetadata tm)
      throws IOException, SourceException, TException {
    if (tm.constructor == 1) {
      this.table = tm.tableName;
      this.database = tm.database;
      this.trashBaseLocation = getCreateTrashBaseLocation("Compaction");
      processTable(database, table);
    }
    if (tm.constructor == 2)
      processLocation(tm.path.toString());
  }
}

