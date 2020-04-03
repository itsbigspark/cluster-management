package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TstDataSetup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;


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

    public OffloadJobIntegrationTests() throws IOException, MetaException, ConfigurationException, SourceException {

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

    void execute() throws ConfigurationException, IOException, MetaException, ParseException, SourceException {
        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.offload.testingDatabase");
        this.metatable = jobProperties.getProperty("com.bigspark.cloudera.management.services.offload.metatable");
        TstDataSetup tstDataSetup = new TstDataSetup();
        tstDataSetup.setUp(testingDatabase, metatable);
        createMetadataTable();

    }

}
