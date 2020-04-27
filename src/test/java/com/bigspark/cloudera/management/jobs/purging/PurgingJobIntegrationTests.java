package com.bigspark.cloudera.management.jobs.purging;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper_OLD;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TstDataSetup;
import java.io.IOException;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PurgingJobIntegrationTests {

  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public PurgingController purgingController;
  public String testingDatabase;
  public String testFile;
  private String metatable;
  public AuditHelper_OLD auditHelperOLD;
  Logger logger = LoggerFactory.getLogger(getClass());

  public PurgingJobIntegrationTests()
      throws IOException, MetaException, ConfigurationException, SourceException {

    ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
    this.purgingController = new PurgingController();
    this.auditHelperOLD = new AuditHelper_OLD(clusterManagementJob, "EDH Cluster purging test","purging.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelperOLD);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
  }


  private void checkpurgingResult(String database, String table, String sys, String inst,
      int numOffsets, Pattern pattern, int retentionDays) {
    if (pattern == Pattern.EAS) {
      long count = spark.sql(String.format(
          "SELECT distinct edi_business_day from %s.%s where src_sys_id='%s' and src_sys_inst_id='%s'",
          database, table, sys, inst)).count();
      assertEquals(count, (numOffsets - retentionDays));
    } else if (pattern == Pattern.SH) {
      long count = spark.sql(String
          .format("SELECT distinct edi_business_day from %s.%s where src_sys_inst_id='%s'",
              database, table, inst)).count();
      assertEquals(count, (numOffsets - retentionDays));
    }
  }

  public void createMetadataTable() {
    if (!spark.catalog().tableExists(testingDatabase, metatable.split("\\.")[0])) {
      System.out.println("Creating test metadata table...");
      spark.sql("CREATE TABLE IF NOT EXISTS " + metatable
          + " (db_name STRING, tbl_name STRING, retention_period INT, retain_month_end STRING, processing_group INT, active STRING)");
      spark.sql("INSERT INTO " + metatable + " VALUES " +
          "('" + testingDatabase + "','test_table_sh',10,'true',1,'true')," +
          "('" + testingDatabase + "','test_table_eas',10,'true',2,'true')");
      spark.sql("SHOW TABLES").show();
    }
  }


  void execute()
      throws Exception {

    this.testingDatabase = jobProperties
        .getProperty("purging.testingDatabase");
    this.metatable = jobProperties
        .getProperty("purging.metatable");
    TstDataSetup tstDataSetup = new TstDataSetup();
    tstDataSetup.setUp(testingDatabase, metatable);
    createMetadataTable();

    System.out.print(Common.getBannerStart("Execution group 1 - SH"));
    purgingController.execute(1);
    checkpurgingResult(testingDatabase, "test_table_sh", "ADB", "NWB", 100, Pattern.SH, 100);
    checkpurgingResult(testingDatabase, "test_table_sh", "ADB", "UBR", 90, Pattern.SH, 100);
    checkpurgingResult(testingDatabase, "test_table_sh", "ADB", "UBN", 80, Pattern.SH, 100);
    checkpurgingResult(testingDatabase, "test_table_sh", "ADB", "RBS", 70, Pattern.SH, 100);
    System.out.print(Common.getBannerFinish("Execution group 1 - SH"));

    Common.getBannerStart("Execution group 2 - EAS");
    purgingController.execute(2);
    checkpurgingResult(testingDatabase, "test_table_eas", "ADB", "NWB", 100, Pattern.EAS, 100);
    checkpurgingResult(testingDatabase, "test_table_eas", "ADB", "UBR", 90, Pattern.EAS, 100);
    checkpurgingResult(testingDatabase, "test_table_eas", "ADB", "UBN", 80, Pattern.EAS, 100);
    checkpurgingResult(testingDatabase, "test_table_eas", "ADB", "RBS", 70, Pattern.EAS, 100);
    System.out.print(Common.getBannerFinish("Execution group 2 - EAS"));

  }

}
