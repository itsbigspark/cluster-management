package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TestDataSetup;
import com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingController;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.Properties;
import java.util.stream.IntStream;


public class OffloadJobIntegrationTest {

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

    public OffloadJobIntegrationTest() throws IOException, MetaException, ConfigurationException, SourceException {

        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.offloadController = new OffloadController();
        this.auditHelper = new AuditHelper(clusterManagementJob,"Storage offload job test");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    public void createMetadataTable(){
        if (!spark.catalog().tableExists(testingDatabase, metatable.split("\\.")[0])) {
            System.out.println("Creating test metadata table...");
            spark.sql("CREATE TABLE IF NOT EXISTS "+metatable+" (database STRING, table STRING, target_platform STRING, target_bucket STRING)");
            spark.sql("INSERT INTO "+metatable+" VALUES " +
                    "('"+testingDatabase+"','test_table_sh','ECS','s3tab')," +
                    "('"+testingDatabase+"','test_table_eas','ECS','s3tab')");
            spark.sql("SHOW TABLES").show();
        }
    }

    @Test
    void execute() throws ConfigurationException, IOException, MetaException, ParseException, SourceException {
        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.offload.testingDatabase");
        this.metatable = jobProperties.getProperty("com.bigspark.cloudera.management.services.offload.metatable");
        TestDataSetup testDataSetup = new TestDataSetup();
        testDataSetup.setUp(testingDatabase, metatable);
        createMetadataTable();

    }

}
