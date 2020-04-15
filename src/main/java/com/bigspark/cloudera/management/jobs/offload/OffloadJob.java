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
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
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
  String targetTable;
  String targetTablePath;
  String trashBaseLocation;


  private Logger logger = LoggerFactory.getLogger(getClass());

  public OffloadJob() throws MetaException, SourceException, ConfigurationException, IOException {
    this.clusterManagementJob = ClusterManagementJob.getInstance();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job", "offload.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
    this.jobProperties = clusterManagementJob.jobProperties;
    this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    String connStr = this.jobProperties.getProperty("impala.connStr");
    this.impalaHelper = new ImpalaHelper(connStr);
    this.jobAudit = new GenericAuditHelper(this.clusterManagementJob, "purging.auditTable",
        this.impalaHelper);
  }

  int distCP(Path src, Path tgt) throws Exception {
    DistCpOptions options = new DistCpOptions(src, tgt);
    options.setOverwrite(true);
    hadoopConfiguration.set("fs.s3a.endpoint", "object.ecstestdrive.com");
    hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "131855586862166345@ecstestdrive.emc.com");
    hadoopConfiguration
        .set("fs.s3a.awsSecretAccessKey", "Q+f/ypU/Ii6s2tWLmpxyaIVgxT4+rBLWroAO4ufS");
    DistCp distCp = new DistCp(hadoopConfiguration, options);
    return distCp.run(new String[]{src.toString(), tgt.toString()});
  }

  /**
   * Method to calculate the minimum retention date (offload ceiling) All dates before that value
   * should be included into offload scope.
   *
   * @param retentionPeriod
   * @param currentDate
   * @return LocalDate
   */
  LocalDate calculateOffloadCeiling(int retentionPeriod, LocalDate currentDate)
  {
    logger.debug("Retention period : " + retentionPeriod);
    if (retentionPeriod <= 0) {
      logger
          .warn("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
      return currentDate;
    }
    LocalDate offloadCeiling = currentDate.minusDays(retentionPeriod);
    logger.debug(String.format("offload ceiling calculated as '%s'", offloadCeiling));
    return offloadCeiling;
  }

  Boolean checkS3Table() throws TException {
    return hiveMetaStoreClient.tableExists(this.database, this.targetTable);
  }

  void createS3Table() {
    String sql_s = String
        .format("CREATE EXTERNAL TABLE %s.%s LOCATION '%s' as SELECT * FROM %s.%s where 1=0",
            this.database, this.targetTable, this.targetTablePath, this.database, this.sourceTable);
    spark.sql(sql_s);
  }

  void createS3Partition(String partitionName) {
    String sql_s = String.format(
        "ALTER TABLE %s.%s ADD PARTITION IF NOT EXIST ('%s')", this.database, this.targetTable,
        partitionName);
    spark.sql(sql_s);
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
      logger.debug("Dropped partition : " + partition.getValues().toString());
    }
  }

  void trashSourceLocation(String location) throws Exception {
    this.trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("offload", this.database, this.sourceTable);
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
    this.trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("offload");
    URI partitionLocation = new URI(p.getSd().getLocation());
    if (! isDryRun) {
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
    if (this.offloadMetadata.constructor == 1) // Database, table type
    {
      offloadTablePartitions();
    } else if (this.offloadMetadata.constructor == 2) // Location type
    {
      offloadLocation();
    }
  }

  private void offloadTablePartitions() throws SourceException, TException {
    this.sourceDescriptor = new SourceDescriptor(
        metadataHelper.getDatabase(offloadMetadata.database),
        offloadMetadata.tableDescriptor);
    this.pattern = MetadataHelper.getTableType(offloadMetadata.tableDescriptor);
    this.database = offloadMetadata.tableDescriptor.getDatabaseName();
    this.sourceTable = offloadMetadata.tableDescriptor.getTableName();
    this.targetTable = this.sourceTable + "_" + platform;
    this.targetTablePath = "s3a://" + offloadMetadata.targetBucket + "/"
        + pattern + "/"
        + offloadMetadata.database + "/"
        + offloadMetadata.tableName;
    if (this.pattern != null) {
      LocalDate purgeCeiling = this.calculateOffloadCeiling(
          offloadMetadata.hdfsRetention
          , LocalDate.now());
      List<Partition> allOffloadCandidates = MetadataHelper.removePartitionToRetain(
          offloadMetadata.tableDescriptor.getPartitionList()
          , purgeCeiling);
      if (!checkS3Table()) {
        createS3Table();
      }
      allOffloadCandidates.forEach(partition -> {
        processPartition(partition);
      });
    } else {
      logger.error("Table pattern not validated");
    }
  }

  private void processPartition(Partition partition){
    createS3Partition(partition.toString());
    String partitionTarget =
        this.targetTable + MetadataHelper.getPartitionString(partition, this.pattern);
    try {
      distCP(new Path(partition.getSd().getLocation()), new Path(partitionTarget));
      trashSourcePartition(partition);
      dropSourcePartition(partition);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void offloadLocation() throws Exception {
    String sourcePathName = offloadMetadata.sourcePath.getName();
    Path targetPath = new Path("s3a://" + offloadMetadata.targetBucket + "/" + sourcePathName);
    int result = distCP(offloadMetadata.sourcePath, targetPath);
    if (result == 0)
      trashSourceLocation(offloadMetadata.sourcePath.toString());
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

