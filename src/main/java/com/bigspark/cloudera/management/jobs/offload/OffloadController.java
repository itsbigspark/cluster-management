package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.Properties;

public class OffloadController {

    public Properties jobProperties;
    public SparkHelper.AuditedSparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;
    public AuditHelper auditHelper;
    public Boolean isDryRun;

    Logger logger = LoggerFactory.getLogger(getClass());

    public OffloadController() throws IOException, MetaException, ConfigurationException, SourceException {
        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        logger.debug("Filesystem active check: " + fileSystem.exists(new Path("/user/chris/active")));
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    }

    /**
     * Method to fetch metadata table value from properties file
     *
     * @return String
     */
    private Dataset<Row> getOffloadTable() {
        return spark.table("com.bigspark.cloudera.management.services.Offload.metatable");
    }



    public void executeOffloadForDatabase(){}

    public void executeOffloadForTable(){}

    public void executeOffloadForLocation(){}

    public void executeOffloadGroup(int executionGroup) throws ConfigurationException, IOException, MetaException, SourceException {

        auditHelper.startup();
        OffloadJob offloadJob = new OffloadJob();
        auditHelper.completion();
    }
}

