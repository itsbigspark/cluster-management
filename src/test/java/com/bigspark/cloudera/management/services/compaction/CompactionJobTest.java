package com.bigspark.cloudera.management.services.compaction;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import com.bigspark.cloudera.management.services.housekeeping.HousekeepingController;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Test;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Properties;

public class CompactionJobTest {

    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public HousekeepingController housekeepingController;
    public String testingDatabase;
    public AuditHelper auditHelper;

    public CompactionJobTest() throws IOException, MetaException, ConfigurationException, SourceException {
        if (spark.sparkContext().isLocal()) {
            //Required only for derby db weird locking issues
            FileUtils.forceDelete(new File(spark.sparkContext().getSparkHome() + "/metastore_db/db.lck"));
            FileUtils.forceDelete(new File(spark.sparkContext().getSparkHome() + "/metastore_db/dbex.lck"));
        }

        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.housekeepingController = new HousekeepingController();
        this.auditHelper = new AuditHelper(clusterManagementJob);
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    void setUp() throws IOException {
        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.testingDatabase");
        if (spark.catalog().tableExists(testingDatabase,"small_files"))
            spark.sql(String.format("DROP TABLE %s.%s",testingDatabase,"small_files"));
        Dataset<Long> df = this.spark.range(100000).cache();
        Dataset<Row> df2 = df.withColumn("partitionCol", new Column("p1"));
        df2.repartition(200).write().partitionBy("partitionCol").saveAsTable(testingDatabase+".small_files_test");
    }



    @Test
    void execute() throws ConfigurationException, IOException, MetaException, SourceException, NoSuchTableException, NoSuchDatabaseException, ParseException {
        setUp();
        InputStream input = CompactionJobTest.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        CompactionJob compactionJob = new CompactionJob();
        compactionJob.jobProperties = prop;
        System.out.print(Common.getBannerStart("Compaction testing"));
        compactionJob.execute(testingDatabase,"small_files_test");

        System.out.print(Common.getBannerFinish("Compaction testing complete"));
    }

}
