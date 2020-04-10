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
  public Boolean isInitialised;
  public ImpalaHelper impala;

  private int counter;

  public GenericAuditHelper(ClusterManagementJob clusterManagementJob, String tableConfigKey,
      ImpalaHelper impala)
      throws ConfigurationException, IOException, MetaException, SourceException {
    this.clusterManagementJob = clusterManagementJob;
    this.impala = impala;
    initialiseAuditTable(tableConfigKey);
  }

  private void initialiseAuditTable(String tableConfigKey) throws IOException, SourceException {
    this.auditTable = clusterManagementJob.jobProperties.getProperty(tableConfigKey);
    String[] auditTable_ = auditTable.split("\\.");
    //Cannot do an audited spark session without the audit table!
    SparkSession spark = SparkHelper.getSparkSession();
    if (!spark.catalog().tableExists(auditTable_[0], auditTable_[1])) {
      logger.error(String.format("Audit table does not exists: %s", auditTable));
    } else {
      setLogfile();
      isInitialised = true;
    }
    this.spark = SparkHelper.getSparkSession();
  }

  private void setLogfile() throws SourceException {
    String[] auditTable_ = auditTable.split("\\.");
    Table table = clusterManagementJob.metadataHelper.getTable(auditTable_[0], auditTable_[1]);
    logfileLocation = clusterManagementJob.metadataHelper.getTableLocation(table);
    logfileName = clusterManagementJob.applicationID;
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
      impala.invalidateMetadata(this.auditTable);
    } catch (Exception e) {
      logger
          .warn(String.format("Unable to invalidate metadata for audit table %s", this.auditTable));
    }
  }

}
