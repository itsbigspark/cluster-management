package com.bigspark.cloudera.management.services;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.Properties;

public class ClusterManagementJob {

    private static ClusterManagementJob INSTANCE;
    protected Properties jobProperties;
    protected SparkSession spark;
    protected FileSystem fileSystem;
    protected Configuration hadoopConfiguration;
    protected HiveMetaStoreClient hiveMetaStoreClient;
    protected MetadataHelper metadataHelper;
    protected Boolean isDryRun;

    public ClusterManagementJob() throws IOException, MetaException, ConfigurationException {
        String appPrefix = jobProperties.getProperty("com.bigspark.cloudera.management.services.sparkAppNamePrefix");
        this.spark=SparkSession.builder()
                .appName(appPrefix+this.getClass().getName())
                .enableHiveSupport()
                .getOrCreate();
        this.hadoopConfiguration =(spark.sparkContext().hadoopConfiguration());
        this.fileSystem =FileSystem.get(hadoopConfiguration);
        this.hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
        this.metadataHelper = new MetadataHelper();
        this.jobProperties = getClusterManagementProperties();
        this.isDryRun = Boolean.valueOf(String.valueOf(
                jobProperties.getProperty("com.bigspark.cloudera.management.services.isDryRun")
                )
        );
    }

    public synchronized static ClusterManagementJob getInstance() throws IOException, MetaException, ConfigurationException {
        if(INSTANCE == null) INSTANCE = new ClusterManagementJob();
        return INSTANCE;
    }

    public synchronized Properties getClusterManagementProperties() throws IOException {
        if(this.jobProperties == null) {
            jobProperties = PropertyUtils.getPropertiesFile("src/main/resources/config.properties");
        }
        return jobProperties;
    }

}
