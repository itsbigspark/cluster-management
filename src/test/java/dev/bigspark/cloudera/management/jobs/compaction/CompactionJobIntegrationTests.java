package dev.bigspark.cloudera.management.jobs.compaction;

import dev.bigspark.cloudera.management.Common;
import dev.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import dev.bigspark.cloudera.management.common.helpers.AuditHelper;
import dev.bigspark.cloudera.management.common.helpers.SparkHelper;
import dev.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Properties;
import javax.naming.ConfigurationException;

import dev.bigspark.hadoop.exceptions.SourceException;
import dev.bigspark.hadoop.helpers.MetadataHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;

public class CompactionJobIntegrationTests {

  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public CompactionController compactionController;
  public String testingDatabase;
  public String testingTable;
  private String metatable;
  public AuditHelper auditHelper;
  public ClusterManagementJob clusterManagementJob;


  public CompactionJobIntegrationTests()
      throws IOException, MetaException, ConfigurationException, SourceException {
    this.clusterManagementJob = ClusterManagementJob.getInstance();
    this.compactionController = new CompactionController();
    this.auditHelper = new AuditHelper(clusterManagementJob, "Small file compaction test", "compaction.auditTable");
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
  }

  void setUp() throws IOException {
    this.testingDatabase = jobProperties
        .getProperty("compaction.testingDatabase");
    this.metatable = jobProperties
        .getProperty("compaction.metatable");
    this.testingTable = "test_table_compaction";
    if (spark.catalog().tableExists(testingDatabase, testingTable)) {
      spark.sql(String.format("DROP TABLE %s.%s", testingDatabase, testingTable));
    }
    Dataset<Long> df = this.spark.range(100000).cache();
    Dataset<Row> df2 = df.withColumn("partitionCol", functions.lit("p1"));
    df2.repartition(200).write().partitionBy("partitionCol")
        .saveAsTable(testingDatabase + "." + testingTable);
  }

  void execute()
      throws ConfigurationException, IOException, MetaException, SourceException, NoSuchTableException, NoSuchDatabaseException, ParseException {

    this.testingDatabase = jobProperties
        .getProperty("compaction.testingDatabase");
    this.metatable = jobProperties
        .getProperty("compaction.metatable");
//        TestDataSetup testDataSetup = new TestDataSetup();
//        testDataSetup.setUp(testingDatabase, metatable);
    setUp();
    InputStream input = CompactionJobIntegrationTests.class.getClassLoader()
        .getResourceAsStream("config.properties");
    Properties prop = new Properties();
    prop.load(input);
    CompactionJob compactionJob = new CompactionJob();
    compactionJob.jobProperties = prop;
    System.out.print(Common.getBannerStart("Compaction testing"));
    compactionJob.execute(
        new CompactionMetadata(metadataHelper.getTableDescriptor(testingDatabase, testingTable)));
    confirmResult();
    System.out.print(Common.getBannerFinish("Compaction testing complete"));
  }

  void confirmResult() throws IOException, SourceException {
    Table table = this.metadataHelper.getTable(testingDatabase, testingTable);
    ContentSummary contentSummary = clusterManagementJob.fileSystem
        .getContentSummary(new Path(this.metadataHelper.getTableLocation(table)));
    assert contentSummary.getFileCount() < 3;
  }

}
