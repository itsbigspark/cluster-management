package com.bigspark.cloudera.management.jobs.compaction;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TestDataSetup;
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
import org.junit.Before;
import org.junit.jupiter.api.Test;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Properties;

public class CompactionJobIntegrationTest {

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


    public CompactionJobIntegrationTest() throws IOException, MetaException, ConfigurationException, SourceException {
        this.clusterManagementJob = ClusterManagementJob.getInstance();
        this.compactionController = new CompactionController();
        this.auditHelper = new AuditHelper(clusterManagementJob, "Small file compaction test");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    @Before
    void setUp() throws IOException {
        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.compaction.testingDatabase");
        this.metatable = jobProperties.getProperty("com.bigspark.cloudera.management.services.compaction.metatable");
        this.testingTable = "test_table_compaction";
        if (spark.catalog().tableExists(testingDatabase,testingTable))
            spark.sql(String.format("DROP TABLE %s.%s",testingDatabase,testingTable));
        Dataset<Long> df = this.spark.range(100000).cache();
        Dataset<Row> df2 = df.withColumn("partitionCol", functions.lit("p1"));
        df2.repartition(200).write().partitionBy("partitionCol").saveAsTable(testingDatabase+"."+testingTable);
    }

    @Test
    void execute() throws ConfigurationException, IOException, MetaException, SourceException, NoSuchTableException, NoSuchDatabaseException, ParseException {

        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.compaction.testingDatabase");
        this.metatable = jobProperties.getProperty("com.bigspark.cloudera.management.services.compaction.metatable");
//        TestDataSetup testDataSetup = new TestDataSetup();
//        testDataSetup.setUp(testingDatabase, metatable);
        setUp();
        InputStream input = CompactionJobIntegrationTest.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        CompactionJob compactionJob = new CompactionJob();
        compactionJob.jobProperties = prop;
        System.out.print(Common.getBannerStart("Compaction testing"));
        compactionJob.execute(new CompactionMetadata(metadataHelper.getTableDescriptor(testingDatabase,testingTable)));
        confirmResult();
        System.out.print(Common.getBannerFinish("Compaction testing complete"));
    }

    void confirmResult() throws IOException, SourceException {
        Table table = this.metadataHelper.getTable(testingDatabase,testingTable);
        ContentSummary contentSummary = clusterManagementJob.fileSystem.getContentSummary(new Path(this.metadataHelper.getTableLocation(table)));
        assert contentSummary.getFileCount() < 3;
    }

}
