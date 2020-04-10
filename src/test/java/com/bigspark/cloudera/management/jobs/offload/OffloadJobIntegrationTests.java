package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TstDataSetup;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OffloadJobIntegrationTests {

  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public OffloadController offloadController;
  public String testingDatabase;
  public String testFile;
  public String metatable;
  public AuditHelper auditHelper;

  Logger logger = LoggerFactory.getLogger(getClass());

  public OffloadJobIntegrationTests()
      throws IOException, MetaException, ConfigurationException, SourceException {

    ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
    this.offloadController = new OffloadController();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job test", "offload.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
  }

  public void createMetadataTable() {
    if (!spark.catalog().tableExists(testingDatabase, metatable.split("\\.")[0])) {
      System.out.println("Creating test metadata table...");
      spark.sql("CREATE TABLE IF NOT EXISTS " + metatable
          + " (db_name STRING, tbl_name STRING, target_platform STRING, hdfs_retention INT, target_bucket STRING, processing_group INT, active STRING)");
      spark.sql("INSERT INTO " + metatable + " VALUES " +
          "('" + testingDatabase + "','test_table_sh','ECS',30,'s3tab',1,'true')," +
          "('" + testingDatabase + "','test_table_eas','ECS',30,'s3tab',2, 'true')");
      spark.sql("SHOW TABLES").show();
    }
  }

  void execute()
      throws ConfigurationException, IOException, MetaException, ParseException, SourceException {
    this.testingDatabase = jobProperties
        .getProperty("offload.testingDatabase");
    this.metatable = jobProperties
        .getProperty("offload.metatable");
//    TstDataSetup tstDataSetup = new TstDataSetup();
//    tstDataSetup.setUp(testingDatabase, metatable);
    createMetadataTable();
    OffloadController offloadController = new OffloadController();
    offloadController.execute(1);


  }

}
