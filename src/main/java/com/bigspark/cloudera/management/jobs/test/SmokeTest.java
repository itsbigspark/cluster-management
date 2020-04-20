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
import java.util.List;
import java.util.Properties;
import javax.naming.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SmokeTest {

  private static Logger logger = LoggerFactory.getLogger(SmokeTest.class);
  protected  ImpalaHelper impalaHelper;
  protected  Properties jobProperties;
  protected  SparkSession spark;
  protected  FileSystem fileSystem;
  protected  Configuration hadoopConfiguration;
  protected  HiveMetaStoreClient hiveMetaStoreClient;
  protected  MetadataHelper metadataHelper;
  protected  Boolean isDryRun;
  protected  ClusterManagementJob clusterManagementJob;
  //protected final  AuditHelper auditHelper;

  public static void main(String[] args)
      throws Exception {
    logger.info("Starting main method");
    SmokeTest st = new SmokeTest();
    st.execute();
    logger.info("Finished main method");
  }

  public SmokeTest() throws MetaException, SourceException, ConfigurationException, IOException {
    logger.info("Starting constructing SmokeTest Class");
    try {
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
      this.impalaHelper =  ImpalaHelper.getInstanceFromProperties(this.jobProperties);
    } catch (Exception ex) {
      logger.error("Failed to instantiate SmokeTest Class through standard constructor", ex);
      System.exit(1);
    }
    logger.info("Finished constructing SmokeTest Class");
  }


  private void execute() {
    this.testJobProperties();
    this.testImpalaConnection();
    this.testSpark();
    this.testFileSystem();
    this.testPartitionMarking();
  }

  private void testJobProperties() {
    logger.info("Starting test of testJobProperties method");
    try {
      this.clusterManagementJob.jobProperties.forEach((key, value)  -> {
        logger.info(String.format("%s=%s", key, value));
      });
    } catch(Exception ex) {
      logger.error("Failed to read config.properties from resource path or user home area", ex);
      System.exit(1);
    }
    logger.info("Finished Test of testJobProperties method");
  }

  private void testSpark() {
    logger.info("Starting test of testSpark method");
    try {
      int counter = 0;
      String dbName = null;

      for(Row row : this.spark.sql("show databases").collectAsList()) {
        counter++;
        if(counter == 1) {
          dbName = row.getString(0);
        }
      }
      logger.info(String.format("1st DB name from 'show databases' command: %s", dbName));

    } catch(Exception ex) {
      logger.error("Failed to use spark.sql  and read databases", ex);
      System.exit(1);
    }
    logger.info("Finished Test of testSpark method");
  }

  private void testFileSystem() {
    logger.info("Starting test of testFileSystem method");
    try {
       for(FileStatus fs : this.fileSystem.listStatus(this.fileSystem.getHomeDirectory())) {
         logger.info(String.format("FileName: %s", fs.getPath().getName()));
       }

    } catch(Exception ex) {
      logger.error("Failed to enumerate files/directories in users home area", ex);
      System.exit(1);
    }
    logger.info("Finished Test of testFileSystem method");
  }

  private void testAuditLog() {
    logger.info("Starting test of testAuditLog method");
    try {


    } catch(Exception ex) {

    }
    logger.info("Finished Test of testAuditLog method");
  }

  private void testPartitionMarking()  {
    logger.info("Starting test of testPartitionMarking method");

    try {

      String dbName = "xxx_db_test";
      String tblName = "xxx_tbl_test";

      logger.info("Creating DB and table with values in %s.%s", dbName, tblName);

      if(spark.catalog().databaseExists(dbName)) {
        this.spark.catalog().listTables(dbName).collectAsList().forEach(table -> {
          this.spark.sql(String.format("drop table if  exists %s.%s", dbName, table.name()));
        });
        this.spark.sql(String.format("drop database if  exists %s", dbName));
      }
      this.spark.sql(String.format("create database if not exists %s", dbName));
      this.spark.sql(String.format(
          "create table if not exists %s.%s (val string) partitioned by (edi_business_day string)", dbName, tblName));
      this.spark.sql(String.format(
          "insert into %s.%s partition (edi_business_day='2020-01-01') values ('Hello')", dbName, tblName));
      this.spark.sql(String.format(
          "insert into %s.%s partition (edi_business_day='2020-01-02') values ('World')", dbName, tblName));

      for (Partition p : this.hiveMetaStoreClient
          .listPartitions(dbName, tblName,  (short) 10000)) {
        p.getParameters().put("month_end", "true");
        hiveMetaStoreClient.alter_partition(dbName, tblName, p);
        logger.info(String.format("Partition: %s", p.toString()));
      }


      logger.info(String.format("Finished Test of testPartitionMarking method - "
          + "please delete table/db xxx_test.xxx_test after review.  "
          + "Backend Test SQL: \r\n%s", this.getPartitionCheckSql(dbName, tblName)));
    } catch(Exception ex) {
      logger.error("Failed to add month_end markers to partitions", ex);
      System.exit(1);
    }
  }

  private void testImpalaConnection()  {
    logger.info("Starting test of testImpalaConnection method");
    try {
      try (Connection conn = this.impalaHelper.getConnection()) {
        try (Statement stmnt = conn.createStatement()) {
          try (ResultSet rs = stmnt.executeQuery("show databases")) {
            rs.next();
            logger.info(String.format("1st DB name from 'show databases' command: %s",
                String.format(rs.getString(1))));
          }
        }
      }
    } catch (Exception ex) {
      logger.error("Failed to connect to impala and read databases", ex);
      System.exit(1);
    }
    logger.info("Finished Test of testImpalaConnection method");
  }

  private String getPartitionCheckSql(String dbName, String tblName) {
    StringBuilder sb = new StringBuilder();
    sb.append("select t.TBL_NAME, p.part_name, pp.*\r\n");
    sb.append("    from BDH_HIVE_OWNER.DBS d\r\n");
    sb.append("inner join BDH_HIVE_OWNER.TBLS t\r\n");
    sb.append("on d.DB_ID=t.DB_ID\r\n");
    sb.append("inner join BDH_HIVE_OWNER.PARTITIONS p\r\n");
    sb.append("on t.TBL_ID = p.TBL_ID\r\n");
    sb.append("inner join BDH_HIVE_OWNER.PARTITION_PARAMS pp\r\n");
    sb.append("on p.PART_ID = pp.PART_ID\r\n");
    sb.append("and pp.PARAM_KEY='month_end'\r\n");
    sb.append(String.format("where t.TBL_NAME = 'xxx_tbl_test'\r\n", tblName));
    sb.append(String.format("and d.NAME = 'xxx_db_test';\r\n",dbName));
    return sb.toString();
  }



 }
