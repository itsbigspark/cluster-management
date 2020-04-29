package com.bigspark.cloudera.management.jobs.compaction;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.AuditHelper_OLD;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob_OLD;
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
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionController {

  public Properties jobProperties;
  public SparkHelper.AuditedSparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public AuditHelper_OLD auditHelperOLD;
  public Boolean isDryRun;

  Logger logger = LoggerFactory.getLogger(getClass());

  public CompactionController()
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
  }

  /**
   * Method to fetch metadata table value from properties file
   *
   * @return String
   */
  private String getCompactionTable() {
    return jobProperties
        .getProperty("com.bigspark.cloudera.management.services.Compaction.metatable");
  }

  /**
   * Method to pull distinct list of databases for purging scoping
   *
   * @return List<Row>
   */
  private List<Row> getRetentionDatabases() {
    return spark
        .sql("SELECT DISTINCT DATABASE FROM " + getCompactionTable() + " WHERE ACTIVE='true'")
        .collectAsList();
  }

  /**
   * Method to pull distinct list of databases in execution group
   *
   * @return List<Row>
   */
  private List<Row> getCompactionGroupDatabases(int group) {
    logger.info("Now pulling list of databases for group : " + group);
    return spark.sql(
        "SELECT DISTINCT DATABASE FROM " + getCompactionTable() + " WHERE ACTIVE='true' and GROUP="
            + group).collectAsList();
  }

  /**
   * Method to pull list of tables in a specific database for purging
   *
   * @return List<Row>
   */
  private List<Row> getCompactionDataForDatabase(String database, int group) {
    logger.info("Now pulling configuration metadata for all tables in database : " + database);
    return spark.sql("SELECT TABLE, RETENTION_PERIOD, RETAIN_MONTH_END FROM " + getCompactionTable()
        + " WHERE DATABASE = '" + database + "' AND ACTIVE='true' AND GROUP =" + group)
        .collectAsList();
  }


  /**
   * Method to fetch the purging metadata for a specific database
   * Fetch all database tables where * provided in metadata
   * @param database
   * @return RetentionMetadataContainer
   */
  private ArrayList<CompactionMetadata> sourceDatabaseTablesFromMetaTable(String database,
      int group) throws SourceException {
    List<Row> compactionTables = getCompactionDataForDatabase(database, group);
    ArrayList<CompactionMetadata> compactionMetadataList = new ArrayList<>();
    logger.info(compactionTables.size() + " rows returned with a Compaction configuration");
    for (Row table : compactionTables) {
      String tableName = table.get(0).toString();
      if (tableName.equals("*")){
        ArrayList<Table> allTablesFromDatabase = metadataHelper.getAllTablesFromDatabase(database);
        ArrayList<TableDescriptor> allTableDescriptors = metadataHelper
            .getAllTableDescriptors(allTablesFromDatabase);
        allTableDescriptors.forEach(tableDescriptor -> compactionMetadataList.add(new CompactionMetadata(tableDescriptor)));
      } else {
        try {
          Table tableMeta = metadataHelper.getTable(database, tableName);
          TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
          CompactionMetadata compactionMetadata = new CompactionMetadata(tableDescriptor);
          compactionMetadataList.add(compactionMetadata);
        } catch (SourceException e) {
          logger.error(
              tableName + " : provided in metadata configuration, but not found in database..");
        }
      }
    }
    return compactionMetadataList;
  }


  public void executeCompactionForLocation(String location)
      throws SourceException, TException, IOException, ConfigurationException {
    CompactionJob compactionJob = new CompactionJob();
    CompactionMetadata compactionMetadata = new CompactionMetadata(new Path(location));
    compactionJob.execute(compactionMetadata);
  }

  public void executeCompactionGroup(int executionGroup)
      throws ConfigurationException, IOException, MetaException, SourceException {
    CompactionJob CompactionJob = new CompactionJob();
    auditHelperOLD.startup();
    List<Row> retentionGroup = getCompactionGroupDatabases(executionGroup);
    retentionGroup.forEach(retentionRecord -> {
      String database = retentionRecord.get(0).toString();
      ArrayList<CompactionMetadata> compactionMetadataList = new ArrayList<>();
      try {
        compactionMetadataList.addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
      } catch (SourceException e) {
        e.printStackTrace();
      }
      compactionMetadataList.forEach(table -> {
        try {
          CompactionJob.execute(table);
        } catch (SourceException | IOException | TException e) {
          e.printStackTrace();
        }
      });
    });
    auditHelperOLD.completion();
  }
}

