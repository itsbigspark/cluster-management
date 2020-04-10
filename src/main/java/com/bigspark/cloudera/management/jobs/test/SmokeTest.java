package com.bigspark.cloudera.management.jobs.test;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SmokeTest {

  private static Logger logger = LoggerFactory.getLogger(SmokeTest.class);
  public Properties jobProperties;
  public SparkSession spark;
  public FileSystem fileSystem;
  public Configuration hadoopConfiguration;
  public HiveMetaStoreClient hiveMetaStoreClient;
  public MetadataHelper metadataHelper;
  public Boolean isDryRun;
  public ClusterManagementJob clusterManagementJob;
  public AuditHelper auditHelper;

  public static void main(String[] args)
      throws TException, SourceException, ConfigurationException, IOException {
    logger.info("Starting main method");
    SmokeTest st = new SmokeTest();
    st.execute();
    logger.info("Finished main method");
  }

  public SmokeTest() throws MetaException, SourceException, ConfigurationException, IOException {
    logger.info("Starting constructing SmokeTest method");
    this.clusterManagementJob = ClusterManagementJob.getInstance();
    //this.auditHelper = new AuditHelper(clusterManagementJob,"Storage offload job");
    this.spark = clusterManagementJob.spark;
    this.fileSystem = clusterManagementJob.fileSystem;
    this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
    this.metadataHelper = clusterManagementJob.metadataHelper;
    this.isDryRun = clusterManagementJob.isDryRun;
    this.jobProperties = clusterManagementJob.jobProperties;
    this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    logger.info("Finished constructing SmokeTest method");
  }

  private void execute() throws SourceException, TException {
    for (Partition p : this.metadataHelper.getTablePartitions("bddlsold01d", "test_table_sh")) {
      for (String key : p.getParameters().keySet()) {
        String value = p.getParameters().get(key);
        System.out.println(String.format("%s: %s", key, value));
      }
      if (!p.getParameters().containsKey("month-end")) {
        p.getParameters().put("month_end", "true");
        hiveMetaStoreClient.alter_partition("bddlsold01d", "test_table_sh", p);
      }
    }
  }
}
