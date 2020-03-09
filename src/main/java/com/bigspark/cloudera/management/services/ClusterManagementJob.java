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
import java.io.InputStream;
import java.util.Properties;

public class ClusterManagementJob {

    private static ClusterManagementJob INSTANCE;
    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;

    public ClusterManagementJob() throws IOException, MetaException, ConfigurationException {
        getClusterManagementProperties();
        String appPrefix = jobProperties.getProperty("com.bigspark.cloudera.management.services.sparkAppNamePrefix");
        if (this.spark == null)
            this.spark=SparkSession.builder()
                    .appName(appPrefix+this.getClass().getName())
                    .enableHiveSupport()
                    .getOrCreate();
        this.hadoopConfiguration =(spark.sparkContext().hadoopConfiguration());
        this.fileSystem =FileSystem.get(hadoopConfiguration);
        if (this.hiveMetaStoreClient == null)
            this.hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
        this.metadataHelper = new MetadataHelper();
        this.jobProperties = getClusterManagementProperties();
        this.isDryRun = Boolean.valueOf(String.valueOf(
                jobProperties.getProperty("com.bigspark.cloudera.management.services.isDryRun")
                )
        );
    }


    public synchronized static ClusterManagementJob getInstance() throws IOException, MetaException, ConfigurationException {
        if(INSTANCE == null) {
            synchronized (ClusterManagementJob.class) {
                INSTANCE = new ClusterManagementJob();
            }
        }
        return INSTANCE;
    }

    public synchronized Properties getClusterManagementProperties() throws IOException {
        if(this.jobProperties == null) {
//            jobProperties = PropertyUtils.getPropertiesFile("/config.properties");
            InputStream input=ClusterManagementJob.class.getClassLoader().getResourceAsStream("config.properties");
            Properties prop = new Properties();
            prop.load(input);
            jobProperties=prop;
        }
        return jobProperties;
    }

}
