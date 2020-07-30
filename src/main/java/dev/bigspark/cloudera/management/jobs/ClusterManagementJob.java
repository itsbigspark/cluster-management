package dev.bigspark.cloudera.management.jobs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.naming.ConfigurationException;

import dev.bigspark.cloudera.management.common.helpers.SparkHelper;
import dev.bigspark.hadoop.exceptions.SourceException;
import dev.bigspark.hadoop.helpers.FileSystemHelper;
import dev.bigspark.hadoop.helpers.MetadataHelper;
import dev.bigspark.utils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManagementJob {

  Logger logger = LoggerFactory.getLogger(getClass());
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

  private ClusterManagementJob()
      throws IOException, MetaException, ConfigurationException, SourceException {

    this.spark = SparkHelper.getSparkSession();
    this.fileSystem = FileSystemHelper.getConnection();

    this.jobProperties = getClusterManagementProperties();
    this.isDryRun = Boolean.valueOf(String.valueOf(
        jobProperties.getProperty("isDryRun")));
    appPrefix = jobProperties
        .getProperty("sparkAppNamePrefix");

    this.hadoopConfiguration = spark.sparkContext().hadoopConfiguration();

    this.metadataHelper = new MetadataHelper();
    this.hiveMetaStoreClient = metadataHelper.getHiveMetastoreClient();
    this.applicationID = SparkHelper.getSparkApplicationId();
    this.trackingURL = SparkHelper.getSparkSession().sparkContext().uiWebUrl().get();
    //Spark options set into base SparkSession object
    spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true");

  }


  public synchronized static ClusterManagementJob getInstance()
      throws IOException, MetaException, ConfigurationException, SourceException {
    if (INSTANCE == null) {
      synchronized (ClusterManagementJob.class) {
        INSTANCE = new ClusterManagementJob();
      }
    }
    return INSTANCE;
  }

  private synchronized Properties getClusterManagementProperties() throws IOException {
    Properties prop = new Properties();
    try {
      InputStream input = ClusterManagementJob.class.getClassLoader()
          .getResourceAsStream("config.properties");
      prop.load(input);
    } catch (Exception e) {
      logger.info("config.properties not found on classpath, falling back to home area load");
      String configSpec = String
          .format("%s/cluster-management/config.properties", FileSystemHelper.getUserHomeArea());
      logger.info(String.format("Loading from %s", configSpec));
      prop.load(this.fileSystem.open(new Path(configSpec)));
    }

    return prop;
  }

  public void dumpProperties() {
    if(this.jobProperties != null) {
      logger.debug(String.format("Loaded Properties:\r\n", PropertyUtils.dumpProperties(this.jobProperties)));
    }
  }

}
