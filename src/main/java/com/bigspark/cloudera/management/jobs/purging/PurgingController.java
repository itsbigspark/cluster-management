package com.bigspark.cloudera.management.jobs.purging;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.PurgingMetadata;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.purging.PurgingJob;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurgingController {

  public Properties jobProperties;
  public SparkHelper.AuditedSparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public AuditHelper auditHelper;
  public Boolean isDryRun;

  Logger logger = LoggerFactory.getLogger(getClass());

  public PurgingController(Boolean isDryRun)
      throws MetaException, SourceException, ConfigurationException, IOException {
    this();
    this.isDryRun = isDryRun;

  }

  public PurgingController()
      throws IOException, MetaException, ConfigurationException, SourceException {
    ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
    this.auditHelper = new AuditHelper(clusterManagementJob, "EDH Cluster Purging");
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
        .getProperty("Purging.metatable");
  }


  /**
   * Method to pull distinct list of databases in execution group.  If Retention Group is -1 all are
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
  private List<Row> getRetentionDataForDatabase(String database, int group) {
    logger.info("Now pulling configuration metadata for all tables in database : " + database);
    String sql =
        "SELECT DISTINCT TBL_NAME, RETENTION_PERIOD, RETAIN_MONTH_END FROM " + getMetadataTable()
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
   * @return RetentionMetadataContainer
   */
  private ArrayList<PurgingMetadata> sourceDatabaseTablesFromMetaTable(String database,
      int group) throws SourceException {
    List<Row> purgeTables = getRetentionDataForDatabase(database, group);
    ArrayList<PurgingMetadata> PurgingMetadataList = new ArrayList<>();
    logger.info(purgeTables.size() + " tables returned with a purge configuration");
    for (Row table : purgeTables) {
      String tableName = table.get(0).toString();
      Integer retentionPeriod = (Integer) table.get(1);
      boolean isRetainMonthEnd = Boolean.parseBoolean(String.valueOf(table.get(2)));
      try {
        logger.debug(String.format("Getting metadata for Table %s.%s", database, tableName));
        Table tableMeta = metadataHelper.getTable(database, tableName);
        TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
        PurgingMetadata PurgingMetadata = new PurgingMetadata(database, tableName,
            retentionPeriod, isRetainMonthEnd, tableDescriptor);
        PurgingMetadataList.add(PurgingMetadata);
      } catch (SourceException e) {
        logger
            .error(tableName + " : provided in metadata configuration, but not found in database..",
                e);
      }
    }
    return PurgingMetadataList;
  }


  public void execute() throws ConfigurationException, IOException, MetaException, SourceException {
    List<Row> retentionGroup = getGroupDatabases(-1);
    this.execute(retentionGroup, -1);
  }

  public void execute(int executionGroup)
      throws ConfigurationException, IOException, MetaException, SourceException {
    List<Row> retentionGroup = getGroupDatabases(executionGroup);
    this.execute(retentionGroup, executionGroup);
  }

  public void execute(List<Row> databases, int executionGroup)
      throws ConfigurationException, IOException, MetaException, SourceException {
    PurgingJob PurgingJob = new PurgingJob();
    auditHelper.startup();

    databases.forEach(retentionRecord -> {
      String database = retentionRecord.get(0).toString();
      logger.info(String.format("Running Purging for database '%s' and processing group : %s ", database, executionGroup));
      ArrayList<PurgingMetadata> PurgingMetadataList = new ArrayList<>();
      try {
        PurgingMetadataList
            .addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
      } catch (SourceException e) {
        e.printStackTrace();
      }
      PurgingMetadataList.forEach(table -> {
        try {
          logger.info(
              String.format("Running purging for table '%s.%s'", database, table.tableName));
          PurgingJob.execute(table);
        } catch (SourceException | IOException | URISyntaxException | TException | ParseException e) {
          e.printStackTrace();
        }
      });
    });
    auditHelper.completion();
  }
}

