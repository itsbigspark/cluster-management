package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import javax.naming.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericAuditHelper {

  Logger logger = LoggerFactory.getLogger(getClass());
  public ClusterManagementJob clusterManagementJob;
  public SparkSession spark;
  public String logfileLocation;
  public String logfileName;
  public String auditTable;
  public String auditDatabase;
  public Boolean isInitialised;

  private int counter;

  public GenericAuditHelper(ClusterManagementJob clusterManagementJob, String auditDatabase, String auditTable)
      throws ConfigurationException, IOException, MetaException, SourceException {
    this.clusterManagementJob = clusterManagementJob;
    this.auditDatabase=auditDatabase;
    this.auditTable=auditTable;
    initialiseAuditTable();
  }

  private void initialiseAuditTable() throws IOException, SourceException {
    //Cannot do an audited spark session without the audit table!
    SparkSession spark = SparkHelper.getSparkSession();
    if (!spark.catalog().tableExists(auditDatabase, auditTable)) {
      logger.error(String.format("Audit table does not exist : %s", auditTable));
    } else {
      setLogfile();
      isInitialised = true;
    }
    this.spark = SparkHelper.getSparkSession();
  }

  private void setLogfile() throws SourceException {
    String[] auditTable_ = auditTable.split("\\.");
    Table table = this.clusterManagementJob.metadataHelper.getTable(auditDatabase, auditTable);
    this.logfileLocation = this.clusterManagementJob.metadataHelper.getTableLocation(table);
    this.logfileName = this.clusterManagementJob.applicationID;
  }

  public void write(String payload) throws IOException {
    if (isInitialised) {
      counter++;
      SparkHelper.Hdfs.appendFileContent(logfileLocation, logfileName, payload);
      if (counter % 500 == 0) {
        this.invalidateAuditTableMetadata();
      }
    } else {
      logger.warn(String.format("Unable to log record to %s: %s", this.auditTable, payload));
    }
//        TODO - Reinstate above line, remove below   append() not supported for local filesystem ACTUALY CAN SNIFF LOCAL FILE SYSTEM
//        Exception in thread "main" java.io.IOException: Not supported
//        at org.apache.hadoop.fs.ChecksumFileSystem.append(ChecksumFileSystem.java:357)
//        File file = new File(logfileLocation.substring(5)+"/"+logfileName);
//        Files.write(file.toPath(), payload.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  public void invalidateAuditTableMetadata() {
    try {
      this.clusterManagementJob.impalaHelper.invalidateMetadata(this.auditDatabase, this.auditTable);
    } catch (Exception ex) {
      logger
          .warn(String.format("Unable to invalidate metadata for audit table %s", this.auditTable), ex);
    }
  }

}
