package com.bigspark.cloudera.management.services;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.bigspark.cloudera.management.helpers.SparkHelper.*;

public class ClusterManagementJob {

    private static ClusterManagementJob INSTANCE;
    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;

    public Boolean isDryRun;
    public String appPrefix;
    public String applicationID;
    public String trackingURL;

    private ClusterManagementJob() throws IOException, MetaException, ConfigurationException, SourceException {
        this.jobProperties = getClusterManagementProperties();
        this.isDryRun = Boolean.valueOf(String.valueOf(
                jobProperties.getProperty("com.bigspark.cloudera.management.services.isDryRun")));
        appPrefix = jobProperties.getProperty("com.bigspark.cloudera.management.services.sparkAppNamePrefix");
        this.spark = SparkHelper.getSparkSession();
        this.hadoopConfiguration =spark.sparkContext().hadoopConfiguration();
        this.fileSystem = FileSystemHelper.getConnection();
        this.metadataHelper = new MetadataHelper();
        this.hiveMetaStoreClient = metadataHelper.getHiveMetastoreClient();
        this.applicationID = getSparkApplicationId();
        this.trackingURL = getSparkSession().sparkContext().uiWebUrl().get();
        //Spark options set into base SparkSession object
        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true");
    }


    public synchronized static ClusterManagementJob getInstance() throws IOException, MetaException, ConfigurationException, SourceException {
        if(INSTANCE == null) {
            synchronized (ClusterManagementJob.class) {
                INSTANCE = new ClusterManagementJob();
            }
        }
        return INSTANCE;
    }

    private synchronized Properties getClusterManagementProperties() throws IOException {
        InputStream input = ClusterManagementJob.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        return prop;
    }

}
