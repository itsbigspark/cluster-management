package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.enums.Platform;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.OffloadMetadata;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.GenericAuditHelper;
import com.bigspark.cloudera.management.helpers.ImpalaHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadJob {

  private final ImpalaHelper impalaHelper;
  private final GenericAuditHelper jobAudit;
  public Properties jobProperties;
  public SparkHelper.AuditedSparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public ClusterManagementJob clusterManagementJob;
  public AuditHelper auditHelper;
  public OffloadMetadata offloadMetadata;
  public SourceDescriptor sourceDescriptor;

  Platform platform;
  Pattern pattern;
  String database;
  String sourceTable;
  Table sourceTableObj;
  String targetTable;
  Table targetTableObj;
  Path targetTablePath;
  String trashBaseLocation;


  private Logger logger = LoggerFactory.getLogger(getClass());

  public OffloadJob() throws MetaException, SourceException, ConfigurationException, IOException {
    this.clusterManagementJob = ClusterManagementJob.getInstance();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job",
        "offload.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.hadoopConfiguration.set("fs.s3a.endpoint", "object.ecstestdrive.com");
    this.hadoopConfiguration
        .set("fs.s3a.awsAccessKeyId", "131855586862166345@ecstestdrive.emc.com");
    this.hadoopConfiguration
        .set("fs.s3a.awsSecretAccessKey", "Q+f/ypU/Ii6s2tWLmpxyaIVgxT4+rBLWroAO4ufS");
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
    this.jobProperties = clusterManagementJob.jobProperties;
    this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    String connStr = this.jobProperties.getProperty("impala.connStr");
    this.impalaHelper = new ImpalaHelper(connStr);
    this.jobAudit = new GenericAuditHelper(this.clusterManagementJob, "purging.auditTable",
        this.impalaHelper);
  }

  boolean hadoopFSCopy(Path srcPath, Path dstPath) throws IOException {
    logger.debug("Now executing hadoop.fs.copy");
    logger.debug("src: " + srcPath.toString() + " dst: " + dstPath.toString());
    this.spark.sparkContext().hadoopConfiguration()
        .set("mapreduce.fileoutputcommitter.algorithm.version", "2");
    FileSystem srcFs = FileSystem.get(srcPath.toUri(), spark.sparkContext().hadoopConfiguration());
    RemoteIterator<LocatedFileStatus> sourceFiles = srcFs.listFiles(srcPath, true);
    FileSystem dstFs = FileSystem.get(dstPath.toUri(), spark.sparkContext().hadoopConfiguration());
    boolean copySuccess = true;
    if (sourceFiles != null) {
      while (sourceFiles.hasNext()) {
        boolean copy = FileUtil.copy(srcFs, sourceFiles.next().getPath(), dstFs, dstPath, false,
            this.hadoopConfiguration);
        if (!copy) {
          copySuccess = false;
          logger.error("Encountered an error during transfer");
          break;
        }
      }
    }
    return copySuccess;
  }

  void sparkWrite(Path srcPath, Path dstPath) throws IOException {
    logger.debug("Now executing Spark S3A df.write");
    logger.debug("src: " + srcPath.toString() + " dst: " + dstPath.toString());
    Dataset<Row> src = this.spark.read().parquet(srcPath.toString());
    logger.debug("df rows : " + src.count());
    src.write().mode("overwrite").parquet(dstPath.toString());
  }

  int distCP(Path srcPath, Path dstPath) throws Exception {
    logger.debug("Now executing distcp");
    logger.debug("src: " + srcPath.toString() + " dst: " + dstPath.toString());
    DistCpOptions options = new DistCpOptions(srcPath, dstPath);
    options.setOverwrite(true);
    DistCp distCp = new DistCp(this.hadoopConfiguration, options);
    return distCp.run(new String[]{srcPath.toString(), dstPath.toString()});
  }

  int distCP(List<Path> srcPaths, Path dstPath) throws Exception {
    logger.debug("Now executing distcp with source path listing");
    logger.debug("src: " + srcPaths.toString());
    logger.debug("dst: " + dstPath.toString());
    DistCpOptions options = new DistCpOptions(srcPaths, dstPath);
    DistCp distCp = new DistCp(this.hadoopConfiguration, options);
    Job job = new Job();
    try {
      job = distCp.execute();
      logger.info("distCp tracking URL : " + job.getTrackingURL());
      logger.info("distCp completed successfully");
      return 0;
    } catch (Exception e) {
      logger.info("distCp tracking URL : " + job.getTrackingURL());
      logger.error("Unexpected error encountered");
      e.printStackTrace();
      return 1;
    }
  }

  /**
   * Method to calculate the minimum retention date (offload ceiling) All dates before that value
   * should be included into offload scope.
   *
   * @param retentionPeriod
   * @param currentDate
   * @return LocalDate
   */
  LocalDate calculateOffloadCeiling(int retentionPeriod, LocalDate currentDate) {
    logger.debug("Retention period : " + retentionPeriod);
    if (retentionPeriod <= 0) {
      logger
          .warn("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
      return currentDate;
    }
    LocalDate offloadCeiling = currentDate.minusDays(retentionPeriod);
    logger.debug(String.format("Offload ceiling calculated as '%s'", offloadCeiling));
    return offloadCeiling;
  }


  Boolean checkS3Table() throws TException, SourceException {
    if (hiveMetaStoreClient.tableExists(this.database, this.targetTable)) {
      this.targetTableObj = metadataHelper.getTable(this.database, this.targetTable);
      logger.debug("Confirmed S3 Table exists : " + this.database + "." + this.targetTable);
      return true;
    } else {
      logger.debug("S3 Table does not exist : " + this.database + "." + this.targetTable);
    }
    return false;
  }

  void createS3Table() throws SourceException {
    logger.info("Creating S3 table : " + this.database + "." + this.targetTable);
    String sql_s = String
        .format("CREATE TABLE %s.%s LIKE %s.%s LOCATION '%s'",
            this.database, this.targetTable, this.database, this.sourceTable, this.targetTablePath);
    spark.sql(sql_s);
    this.targetTableObj = metadataHelper.getTable(this.database, this.targetTable);
    logger.debug("Created table : "+targetTableObj.toString());
  }

  Partition createS3Partition(Partition p) throws TException {
//    String sql_s = String.format(
//        "ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s/%s'", this.database, this.targetTable,
//        MetadataHelper.quotePartitionValue(partitionName), targetTablePath, partitionName);
//    spark.sql(sql_s);
    Partition part = new Partition();
    part.setDbName(this.targetTableObj.getDbName());
    part.setTableName(this.targetTableObj.getTableName());
    part.setValues(p.getValues());
    part.setParameters(new HashMap<>());
    part.setSd(this.targetTableObj.getSd().deepCopy());
    part.getSd().setSerdeInfo(this.targetTableObj.getSd().getSerdeInfo());
    String partitionPathString = getPartitionStringFromLocationAndTableLocation(
        sourceTableObj.getSd().getLocation()
        ,p.getSd().getLocation()
    );
    part.getSd().setLocation(this.targetTableObj.getSd().getLocation() + "/"
        + partitionPathString
    );
    try {
      Partition partition = hiveMetaStoreClient.add_partition(part);
      logger.debug("S3 Partition created : " + part.toString());
      return partition;
    } catch (AlreadyExistsException e) {
      logger.debug("S3 Table Partition already exists : " + part.toString());
    }
    return part;
  }

  /**
   * Method to drop table partition using HiveMetaStoreClient
   *
   * @param partition
   * @throws TException
   */
  void dropSourcePartition(Partition partition) throws TException {
    if (this.isDryRun) {
      logger.info("DRY RUN - Dropped partition : " + partition.getValues().toString());
    } else {
      MetadataHelper.dropHivePartition(partition, this.hiveMetaStoreClient);
      logger.debug("Dropped HDFS partition : " + partition.toString());
    }
  }

  void trashSourceLocation(String location) throws Exception {
    URI sourceLocation = new URI(location);
    FileSystemHelper.moveDataToUserTrashLocation(
        sourceLocation.getPath(),
        trashBaseLocation,
        this.isDryRun,
        this.fileSystem);

    String payload = this.getAuditLogRecord(
        sourceLocation.getPath()
        , trashBaseLocation);

    this.jobAudit.write(payload);
  }

  void trashSourcePartition(Partition p) throws Exception {
    URI partitionLocation = new URI(p.getSd().getLocation());
    if (!isDryRun) {
      FileSystemHelper.moveDataToUserTrashLocation(
          partitionLocation.getPath(),
          trashBaseLocation,
          this.isDryRun,
          this.fileSystem);
    }
    String payload = this.getAuditLogRecord(
        this.offloadMetadata.database
        , this.offloadMetadata.tableName
        , partitionLocation.getPath()
        , trashBaseLocation);

    this.jobAudit.write(payload);
  }
  String getPartitionStringFromLocationAndTableLocation(String tableLocation, String partitionLocation){
    return partitionLocation.substring(tableLocation.length(),partitionLocation.length());
  }

  void invalidateMetadata() {
  }

  /**
   * Main entry point method for executing the offload process
   *
   * @throws MetaException
   * @throws SourceException
   */
  void execute(OffloadMetadata offloadMetadata) throws Exception {
    this.offloadMetadata = offloadMetadata;
    this.platform = offloadMetadata.platform;
    this.trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("offload");
    this.hadoopConfiguration.set("fs.s3a.endpoint", "object.ecstestdrive.com");
    this.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "131855586862166345@ecstestdrive.emc.com");
    this.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "Q+f/ypU/Ii6s2tWLmpxyaIVgxT4+rBLWroAO4ufS");
    if (this.offloadMetadata.constructor == 1) // Database, table type
    {
      offloadTablePartitions();
    } else if (this.offloadMetadata.constructor == 2) // Location type
    {
      offloadLocation();
    }
  }

  private void offloadTablePartitions() throws Exception {
    this.sourceDescriptor = new SourceDescriptor(
        metadataHelper.getDatabase(offloadMetadata.database),
        offloadMetadata.tableDescriptor);
    this.pattern = MetadataHelper.getTableType(offloadMetadata.tableDescriptor);
    this.database = offloadMetadata.tableDescriptor.getDatabaseName();
    this.sourceTable = offloadMetadata.tableDescriptor.getTableName();
    this.sourceTableObj = metadataHelper.getTable(this.database, this.sourceTable);
    this.targetTable = this.sourceTable + "_" + platform;
    this.targetTablePath = new Path("s3a://" + offloadMetadata.targetBucket + "/"
        + pattern + "/"
        + offloadMetadata.database + "/"
        + offloadMetadata.tableName);
    if (this.pattern != null) {
      LocalDate offloadCeiling = this.calculateOffloadCeiling(
          offloadMetadata.hdfsRetention
          , LocalDate.now());
      List<Partition> allOffloadCandidates = MetadataHelper.removePartitionToRetain(
          offloadMetadata.tableDescriptor.getPartitionList()
          , offloadCeiling);
      if (!checkS3Table()) {
        createS3Table();
      }
      bulkProcessPartitions(allOffloadCandidates);
    } else {
      logger.error("Table pattern not validated");
    }
  }

  void measureTransfer(long startTime, String type) {
    long stopTime = System.nanoTime();
    long elapsedTime = (stopTime - startTime);
    double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000;
    logger.debug(type + " : Transfer took : " + elapsedTimeInSecond + " seconds");
  }

  void executeSingleTransfer(Partition hdfsPartition, Partition s3Partition) throws Exception {
    String partitionTarget = s3Partition.getSd().getLocation() + "/" + MetadataHelper
        .getPartitionString(hdfsPartition, this.pattern);
    long startTime1 = System.nanoTime();
    distCP(new Path(hdfsPartition.getSd().getLocation()), new Path(partitionTarget));
    measureTransfer(startTime1, "distCP");
    long startTime2 = System.nanoTime();
    hadoopFSCopy(new Path(hdfsPartition.getSd().getLocation()), new Path(partitionTarget + "/"));
    measureTransfer(startTime2, "hadoop.fs.copy");
    long startTime3 = System.nanoTime();
    sparkWrite(new Path(hdfsPartition.getSd().getLocation()), new Path(partitionTarget + "/"));
    measureTransfer(startTime3, "spark.write");
  }


  private void processPartition(Partition hdfsPartition) throws TException {
    Partition s3Partition = createS3Partition(hdfsPartition);
    try {
      executeSingleTransfer(hdfsPartition, s3Partition);
    } catch (Exception e) {
      logger.error("Error encountered processing partition : " + hdfsPartition.toString());
      e.printStackTrace();
    }
  }

  private void bulkProcessPartitions(List<Partition> allOffloadCandidates) throws Exception {
    ArrayList<Path> sourcePaths = new ArrayList<>();
    allOffloadCandidates
        .forEach(partition -> sourcePaths.add(new Path(partition.getSd().getLocation())));
    int distcpReturnCode = distCP(sourcePaths, this.targetTablePath);
    if (distcpReturnCode != 0){
      System.exit(distcpReturnCode);
    }
    allOffloadCandidates.forEach(partition -> {
      try {
        createS3Partition(partition);
        trashSourcePartition(partition);
        dropSourcePartition(partition);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  private void offloadLocation() throws Exception {
    String sourcePathName = offloadMetadata.sourcePath.getName();
    Path targetPath = new Path("s3a://" + offloadMetadata.targetBucket + "/" + sourcePathName);
    int result = distCP(offloadMetadata.sourcePath, targetPath);
    if (result == 0) {
      trashSourceLocation(offloadMetadata.sourcePath.toString());
    }
  }

  private String getAuditLogRecord(String originalLocation, String trashLocation) {
    return String.format("%s~%s~%s~%s~%s~%s~%s\n"
        , clusterManagementJob.applicationID
        , ""
        , ""
        , "specific_location"
        , originalLocation
        , trashLocation
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    );
  }

  private String getAuditLogRecord(String dbName, String tableName, String originalLocation,
      String trashLocation) {
    return String.format("%s~%s~%s~%s~%s~%s~%s\n"
        , clusterManagementJob.applicationID
        , dbName
        , tableName
        , "partition_spec"
        , originalLocation
        , trashLocation
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    );
  }
}


