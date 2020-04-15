package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.enums.Platform;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.OffloadMetadata;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadController {

  public Properties jobProperties;
  public SparkHelper.AuditedSparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public AuditHelper auditHelper;
  public Boolean isDryRun;

  Logger logger = LoggerFactory.getLogger(getClass());

  public OffloadController(Boolean isDryRun)
      throws MetaException, SourceException, ConfigurationException, IOException {
    this();
    this.isDryRun = isDryRun;
  }

  public OffloadController()
      throws IOException, MetaException, ConfigurationException, SourceException {
    ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job", "offload.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
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
   * @return String fully qualified Name ([db].[tbl]) of the Retention table
   */
  private String getMetadataTable() {
    return jobProperties
        .getProperty("offload.metatable");
  }


  /**
   * Method to pull distinct list of databases in processing group.  If processinf Group is -1 all are
   * returned
   *
   * @return List<Row> A list of database names within the specified retention group
   */
  private List<Row> getGroupDatabases(int group) {
    logger.info("Now pulling list of databases for group : " + group);
    String sql = "SELECT DISTINCT DB_NAME FROM " + getMetadataTable() + " WHERE ACTIVE='true'";
    if (group > 0) {
      sql += " and PROCESSING_GROUP=" + group;
    }
    logger.debug("Returning DB Config SQL: " + sql);
    return spark.sql(sql).collectAsList();
  }



  /**
   * Method to pull list of tables within a specific database for purging
   *
   * @param database
   * @param group
   * @return List<Row> List of Records for the given DB/Group
   */
  private List<Row> getMetaDataForDatabase(String database, int group) {
    logger.info("Now pulling configuration metadata for all tables in database : " + database);
    String sql =
        "SELECT DISTINCT TBL_NAME,TARGET_PLATFORM, TARGET_BUCKET, HDFS_RETENTION FROM " + getMetadataTable()
            + " WHERE DB_NAME = '" + database + "' AND LOWER(ACTIVE)='true'";
    if (group >= 0) {
      sql += " AND PROCESSING_GROUP =" + group;
    }
    logger.debug("Returning DB Table Config SQL: " + sql);
    return spark.sql(sql).collectAsList();
  }


  /**
   * Method to fetch the Purging metadata for a specific database
   *
   * @param database
   * @param group
   * @return OffloadMetadata
   */
  private ArrayList<OffloadMetadata> sourceDatabaseTablesFromMetaTable(String database,
      int group) throws SourceException {
    List<Row> purgeTables = getMetaDataForDatabase(database, group);
    ArrayList<OffloadMetadata> offloadMetadataList = new ArrayList<>();
    logger.info(purgeTables.size() + " tables returned with a offload configuration");
    for (Row table : purgeTables) {
      String tableName = table.get(0).toString();
      Platform platform = Platform.valueOf(table.get(1).toString());
      String bucket = table.get(2).toString();
      Integer hdfsRetention = (Integer) table.get(3);
      try {
        logger.debug(String.format("Getting metadata for Table : %s.%s", database, tableName));
        Table tableMeta = metadataHelper.getTable(database, tableName);
        TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
        OffloadMetadata offloadMetadata = new OffloadMetadata(tableDescriptor,platform,bucket,hdfsRetention);
        offloadMetadataList.add(offloadMetadata);
      } catch (SourceException e) {
        logger
            .error(tableName + " : provided in metadata configuration, but not found in database..",
                e);
      }
    }
    return offloadMetadataList;
  }



  public void execute() throws MetaException, SourceException, ConfigurationException, IOException {
    List<Row> processGroup = getGroupDatabases(-1);
    this.execute(processGroup, -1);
  }

  public void execute(int executionGroup)
      throws MetaException, SourceException, ConfigurationException, IOException {
    List<Row> offloadGroup = getGroupDatabases(executionGroup);
    this.execute(offloadGroup, executionGroup);
  }

  public void execute(String location, String platform, String bucket)
      throws Exception {
    auditHelper.startup();
    OffloadJob offloadJob = new OffloadJob();
    OffloadMetadata offloadMetadata = new OffloadMetadata(new Path(location),Platform.valueOf(platform),bucket);
    logger.info(String.format("Running offload for location : '%s'", location));
    logger.debug(offloadMetadata.toString());
    offloadJob.execute(offloadMetadata);

  }


  private void execute(List<Row> offloadGroup, int executionGroup)
      throws MetaException, SourceException, ConfigurationException, IOException {
    OffloadJob offloadJob = new OffloadJob();
    auditHelper.startup();
    offloadGroup.forEach(offloadRecord -> {
      String database = offloadRecord.get(0).toString();
      logger.info(String.format("Running offload for database : '%s' and processing group : '%s'", database, executionGroup));
      ArrayList<OffloadMetadata> offloadMetadataList = new ArrayList<>();
      try {
        offloadMetadataList
            .addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
      } catch (SourceException e) {
        e.printStackTrace();
      }
      offloadMetadataList.forEach(table -> {
        try {
          logger.info(
              String.format("Running offload for table : '%s.%s'", database, table.tableName));
          logger.debug(table.toString());
          offloadJob.execute(table);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    auditHelper.completion();
  }
}

