package dev.bigspark.cloudera.management.jobs;

import dev.bigspark.cloudera.management.common.helpers.AuditHelper;
import dev.bigspark.cloudera.management.common.helpers.SparkHelper;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Properties;
import java.util.stream.IntStream;
import javax.naming.ConfigurationException;

import dev.bigspark.hadoop.exceptions.SourceException;
import dev.bigspark.hadoop.helpers.FileSystemHelper;
import dev.bigspark.hadoop.helpers.MetadataHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TstDataSetup {

  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public String testingDatabase;
  public String testFile;
  public String metatable;
  public AuditHelper auditHelper;

  Logger logger = LoggerFactory.getLogger(getClass());

  public TstDataSetup() throws IOException, MetaException, ConfigurationException, SourceException {
    ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Test data setup", "purging.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
  }

  public void setUp(String testingDatabase, String metatable) throws IOException {
    this.testingDatabase = testingDatabase;
    this.metatable = metatable;

    String userHomeArea;
    String fileName = "testdata.csv";

    if (spark.sparkContext().hadoopConfiguration().size() > 0) {
      userHomeArea = FileSystemHelper.getUserHomeArea();
    } else {
      userHomeArea = "/tmp";
    }
    this.testFile = userHomeArea + "/" + fileName;

    logger.info("Cleaning up last run");
    dropTables();

    logger.info("Now generating test dataset");
    StringBuilder sb = new StringBuilder();
    sb.append("value,edi_business_day,src_sys_id,src_sys_inst_id\n");

    generateTestData("ADB", "NWB", 100, sb);
    generateTestData("ADB", "UBR", 90, sb);
    generateTestData("ADB", "UBN", 80, sb);
    generateTestData("ADB", "RBS", 70, sb);
    System.out.println(sb.toString());

    if (spark.sparkContext().master().equals("local")
        && spark.sparkContext().hadoopConfiguration().size() == 0) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFile))) {
        writer.write(sb.toString());
      }
    } else if (spark.sparkContext().hadoopConfiguration().size() > 0) {
      FileSystemHelper.writeFileContent(userHomeArea, fileName, sb.toString(), true);
    }
    createTestTables();
  }

  private void dropTables() throws IOException {
    spark.sql("DROP TABLE IF EXISTS " + testingDatabase + ".test_table_eas");
    spark.sql("DROP TABLE IF EXISTS " + testingDatabase + ".test_table_sh");
    spark.sql("DROP TABLE IF EXISTS " + testingDatabase + ".test_table_eas_swing");
    spark.sql("DROP TABLE IF EXISTS " + testingDatabase + ".test_table_sh_swing");
    spark.sql("DROP TABLE IF EXISTS " + metatable);
  }


  private String generateBusinessDate(int offset) {
    return LocalDate.now().minusDays(offset).toString();
  }

  private StringBuilder addData(String sys, String inst, int offset) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("test_%d,%s,%s,%s\n", offset, generateBusinessDate(offset), sys, inst));
    return sb;
  }

  private StringBuilder generateTestData(String sys, String inst, int numOffsets,
      StringBuilder sb) {
    IntStream.range(0, numOffsets).forEach(i ->
        sb.append(addData(sys, inst, i)).toString()
    );
    return sb;
  }

  private void createTestTables() throws IOException {
    System.out.println("Check test tables exist...");
    spark.sql("USE " + testingDatabase);
    spark.sql("SHOW TABLES").show();
    spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true");
    Dataset<Row> csv = spark.read().option("header", "true").csv(testFile);
    csv.cache();
    if (!spark.catalog().tableExists(testingDatabase, "test_table_sh")) {
      System.out.println("Creating SH test table...");
      csv.write().partitionBy("edi_business_day").mode("overwrite")
          .saveAsTable(testingDatabase + ".test_table_sh");
      spark.sql("SHOW TABLES").show();
    }
    if (!spark.catalog().tableExists(testingDatabase, "test_table_eas")) {
      System.out.println("Creating EAS test table...");
      csv.write().partitionBy("edi_business_day", "src_sys_id", "src_sys_inst_id").mode("overwrite")
          .saveAsTable(testingDatabase + ".test_table_eas");
      spark.sql("SHOW TABLES").show();
    }
  }

}
