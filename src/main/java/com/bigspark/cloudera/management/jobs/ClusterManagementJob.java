package com.bigspark.cloudera.management.jobs;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.GenericAuditHelper;
import com.bigspark.cloudera.management.helpers.ImpalaHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClusterManagementJob {

  Logger logger = LoggerFactory.getLogger(getClass());

  public final String cJOB_AUDIT_TABLE_NAME ="SYS_CM_JOB_AUDIT";


  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public ImpalaHelper impalaHelper;
  public String managementDb;
  public String managementDbLocation;
  public GenericAuditHelper jobLog;

  public Boolean isDryRun;
  public String appPrefix;
  public String applicationID;
  public String trackingURL;

  protected ClusterManagementJob()
      throws Exception {
    //Spark Initialisation
    this.spark = this.getSparkSession();
    this.applicationID = this.spark.sparkContext().applicationId();
    this.trackingURL = this.spark.sparkContext().uiWebUrl().get();
    this.spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true");

    //HDFS initialisation
    this.fileSystem = this.getFileSystem();

    //Property Initialisation
    this.jobProperties = this.getClusterManagementProperties();
    this.isDryRun = Boolean.valueOf(String.valueOf(this.jobProperties.getProperty("isDryRun")));
    if(this.isDryRun == null) {
      logger.warn("isDryRun is null, defaulting to false");
      this.isDryRun = false;
    }
    this.appPrefix = this.jobProperties.getProperty("sparkAppNamePrefix");

    //Configuration initialisation
    this.hadoopConfiguration = this.spark.sparkContext().hadoopConfiguration();

    this.metadataHelper = new MetadataHelper();
    this.hiveMetaStoreClient = metadataHelper.getHiveMetastoreClient();

    this.impalaHelper = this.getImpalaHelper();

    this.managementDb = this.getManagementDatabase();
    this.managementDbLocation = this.getManagementDatabaseLocation();

    this.initialiseJobAuditTable();
    this.jobLog = new GenericAuditHelper(
        this
        , this.managementDb
        , this.cJOB_AUDIT_TABLE_NAME);
  }



  protected ClusterManagementJob(ClusterManagementJob existing) {
    this.spark=existing.spark;
    this.fileSystem=existing.fileSystem;
    this.jobProperties=existing.jobProperties;
    this.isDryRun=existing.isDryRun;
    this.appPrefix=existing.appPrefix;
    this.hadoopConfiguration=existing.hadoopConfiguration;
    this.metadataHelper=existing.metadataHelper;
    this.hiveMetaStoreClient=existing.hiveMetaStoreClient;
    this.applicationID=existing.applicationID;
    this.trackingURL=existing.trackingURL;
    this.managementDb = existing.managementDb;
    this.managementDbLocation=existing.managementDbLocation;
    this.impalaHelper=existing.impalaHelper;
    this.jobLog=existing.jobLog;
  }

  private synchronized Properties getClusterManagementProperties() throws IOException {
    Properties prop = new Properties();
    try {
      InputStream input = ClusterManagementJob.class.getClassLoader()
          .getResourceAsStream("config.properties");
      prop.load(input);
    } catch (Exception e) {
      logger.info("config.properties not found on classpath, falling back to home area load");
      String configSpec = String
          .format("%s/cluster-management/config.properties", FileSystemHelper.getUserHomeArea());
      logger.info(String.format("Loading from %s", configSpec));
      prop.load(this.fileSystem.open(new Path(configSpec)));
    }
    return prop;
  }

  public void dumpProperties() {
    if(this.jobProperties != null) {
      logger.debug(String.format("Loaded Properties:\r\n",PropertyUtils.dumpProperties(this.jobProperties)));
    }
  }

  private SparkSession getSparkSession() {
    logger.debug("Getting Spark Session");
    SparkSession sparkTmp = SparkHelper.getSparkSession();
    logger.debug(String.format("Application ID: %s" , sparkTmp.sparkContext().applicationId()));
    logger.debug(String.format("Tracking URL: %s" , sparkTmp.sparkContext().uiWebUrl().get()));
    return sparkTmp;
  }

  public FileSystem getFileSystem() throws IOException {
    logger.debug("Getting File System");
    FileSystem fs = FileSystemHelper.getConnection();
    logger.debug(String.format("Home Area: %s" ,fs.getHomeDirectory().toString()));
    return fs;
  }

  public ImpalaHelper getImpalaHelper() throws Exception {
    logger.debug("Getting Impala");
    ImpalaHelper impala  = ImpalaHelper.getInstanceFromProperties(this.jobProperties);
    return impala;
  }

  public String getManagementDatabase() {
    String dbName = "default";
    String configKey = "management.database";

    if(this.jobProperties.containsKey(configKey)) {
      return this.jobProperties.getProperty("management.database", dbName);
    } else {
      logger.warn(String.format("Could not locate management in config.properties using key '%s', using 'default'"
          , configKey));
    }

    return dbName;
  }

  public String getManagementDatabaseLocation() {
    String retVal = null;
    String configKey = "management.database.location";

    if(this.jobProperties.containsKey(configKey)) {
      return this.jobProperties.getProperty(configKey);
    } else {
      logger.warn(String.format("Could not locate management location in config.properties using key '%s'', falling back to the DB location"
          , configKey));
    }

    return retVal;
  }

  private void initialiseJobAuditTable() throws TException {
    if(!this.spark.catalog().tableExists(this.managementDb, this.cJOB_AUDIT_TABLE_NAME)) {
      logger.info(String.format("Job Audit table does not exist.  Creating %s.%s"
          , this.managementDb
          , this.cJOB_AUDIT_TABLE_NAME));
      spark.sql(this.getAuditTableSql());
    }

    String viewName = String.format("%s_CURRENT_V ", this.cJOB_AUDIT_TABLE_NAME);
    if(!this.spark.catalog().tableExists(this.managementDb, viewName)) {
      logger.info(String.format("Job Audit table view does not exist.  Creating %s.%s_CURRENT_V "
          , this.managementDb
          , this.cJOB_AUDIT_TABLE_NAME));
      spark.sql(this.getAuditTableViewSql());
    }
  }

  private String getAuditTableSql() {
    StringBuilder sql  = new StringBuilder();
    sql.append(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (\n", this.managementDb, this.cJOB_AUDIT_TABLE_NAME));
    sql.append("application_id STRING\n");
    sql.append(",job_type STRING\n");
    sql.append(",database_name STRING\n");
    sql.append(",table_name STRING\n");
    sql.append(",fs_location STRING\n");
    sql.append(",message STRING\n");
    sql.append(",status STRING\n");
    sql.append(",log_time TIMESTAMP\n");
    sql.append(",log_nos bigint\n");
    sql.append(")\n");
    sql.append(" ROW FORMAT DELIMITED\n");
    sql.append(" FIELDS TERMINATED BY '~'\n");
    if(this.managementDbLocation != null  && !this.managementDbLocation.equals("")) {
      sql.append(String.format("LOCATION '%s/data/%s'\n"
          , this.managementDbLocation
          , this.cJOB_AUDIT_TABLE_NAME));
    }
    logger.trace(String.format("Audit Table SQL:\n%s", sql.toString()));
    return sql.toString();
  }

  private String getAuditTableViewSql() {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("CREATE VIEW IF NOT EXISTS %s.%s_CURRENT_V  AS\n", this.managementDb,
        this.cJOB_AUDIT_TABLE_NAME));
    sql.append("select job_type, database_name, table_name, fs_location, status, message, log_time\n");
    sql.append("from\n");
    sql.append(" (\n");
    sql.append("select *\n");
    sql.append(", row_number() over (partition by job_type, database_name, table_name order by log_time desc, log_nos desc) as row_num\n");
    sql.append(String.format("from %s.%s\n", this.managementDb, this.cJOB_AUDIT_TABLE_NAME));
    sql.append(") tbl\n");
    sql.append("where row_num = 1");
    logger.trace(String.format("Audit Table View SQL:\n%s", sql.toString()));
    return sql.toString();
  }


  protected abstract void jobAuditBegin() throws IOException;
  protected abstract void jobAuditEnd() throws IOException;
  protected abstract void jobAuditFail(String error) throws IOException;

  protected void impalaInvalidateMetadata(String dbName, String tableName) {
    this.impalaInvalidateMetadata(dbName, tableName, null);
  }

  /**
   * Method to execute an Invalidate metadata statement on a table for Impala
   */
  protected void impalaInvalidateMetadata(String dbName, String tableName,
      List<Partition> purgeCandidates) {
    if (purgeCandidates == null || purgeCandidates.size() > 0) {
      try {
        logger.debug(String.format("Invalidating  Metadata for %s.%s", dbName, tableName));
        this.impalaHelper.invalidateMetadata(dbName, tableName);
      } catch (Exception ex) {

        if(logger.isDebugEnabled()) {
          logger
              .error(String.format("Unable to invalidate metadata for %s.%s", dbName, tableName), ex);
        } else {
          logger
              .error(String.format("Unable to invalidate metadata for %s.%s.  Turn on DEBUG logging for full Stack Trace", dbName, tableName));
        }
      }
    } else {
      logger.debug(String.format("Skipping Invalidating  Metadata for %s.%s", dbName, tableName));
    }
  }

  protected void impalaComputeStats(String dbName, String tableName, String partitionSpec) {
    try {
      this.impalaHelper.computeStats(dbName, tableName, partitionSpec);
    } catch (Exception ex) {
      logger
          .error(String
              .format("Unable to compute stats for %s.%s partition (%s)", dbName,
                  tableName, partitionSpec), ex);
    }
  }

}
