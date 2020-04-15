package com.bigspark.cloudera.management.jobs.test;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.ImpalaHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SmokeTest {

  private static Logger logger = LoggerFactory.getLogger(SmokeTest.class);
  protected final ImpalaHelper impalaHelper;
  protected final  Properties jobProperties;
  protected final  SparkSession spark;
  protected final  FileSystem fileSystem;
  protected final  Configuration hadoopConfiguration;
  protected final  HiveMetaStoreClient hiveMetaStoreClient;
  protected final  MetadataHelper metadataHelper;
  protected final  Boolean isDryRun;
  protected final  ClusterManagementJob clusterManagementJob;
  //protected final  AuditHelper auditHelper;

  public static void main(String[] args)
      throws Exception {
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
    String connStr = this.jobProperties.getProperty("impala.connStr");
    //String connStr = "jdbc:impala:// mg1dhd015p.server.rbsgrp.net:21050;AuthMech=1;KrbRealm=EUROPA.RBSGRP.NET;KrbHostFQDN= mg1dhd015p.server.rbsgrp.net;KrbServiceName=service-impala-p";
    this.impalaHelper = new ImpalaHelper(connStr);
  }


  private void execute()
      throws Exception {
    testPartitionMarking();
  }

  private void testPartitionMarking() throws Exception {
    String database = "bddlsold01d";
    String table = "test_table_sh";

    for(Partition p : this.hiveMetaStoreClient.listPartitions(database, table, (short)10000)) {

      p.getParameters().put("month_end", "true");
      hiveMetaStoreClient.alter_partition(database, table, p);
      logger.info(p.toString());
    }
  }

  private void testImpalaConnection() throws Exception {
    try (Connection conn = this.impalaHelper.getConnection()) {
      try (Statement stmnt = conn.createStatement()) {
        try(ResultSet rs = stmnt.executeQuery("show databases")) {
          while(rs.next()) {
            logger.debug(String.format(rs.getString(1)));
          }
        }
      }
    }
  }
}
