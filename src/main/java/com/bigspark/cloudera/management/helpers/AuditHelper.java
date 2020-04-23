package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditHelper {

  Logger logger = LoggerFactory.getLogger(getClass());
  public ClusterManagementJob clusterManagementJob;
  public SparkHelper.AuditedSparkSession spark;
  public String logfileLocation;
  public String logfileName;
  public String auditTable;
  public String jobType;

  public AuditHelper(ClusterManagementJob clusterManagementJob, String jobType, String tableConfigKey)
      throws IOException, SourceException {
    this.clusterManagementJob = clusterManagementJob;
    this.jobType = jobType;
    intitialiseAuditTable(tableConfigKey);
    setLogfile();
  }

  private void intitialiseAuditTable(String tableConfigKey) throws IOException {
    if (this.clusterManagementJob == null) {
      logger.warn("cmj is null");
    }
    this.auditTable = clusterManagementJob.jobProperties
        .getProperty(tableConfigKey);
    String[] auditTable_ = auditTable.split("\\.");
    //Cannot do an audited spark session without the audit table!
    SparkSession spark = SparkHelper.getSparkSession();
    if (!spark.catalog().tableExists(auditTable_[0], auditTable_[1])) {
      spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s.%s (" +
              "class_name STRING" +
              ", method_name STRING" +
              ", application_id STRING" +
              ", tracking_url STRING" +
              ", action STRING" +
              ", descriptor STRING" +
              ", message STRING" +
              ", log_time TIMESTAMP" +
              ", status STRING" +
              ") " +
              " ROW FORMAT DELIMITED" +
              " FIELDS TERMINATED BY '~'" +
              " "
          , auditTable_[0], auditTable_[1])
      );
    }
    this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, this);
  }

  private void setLogfile() throws SourceException {
    String[] auditTable_ = auditTable.split("\\.");
    Table table = clusterManagementJob.metadataHelper.getTable(auditTable_[0], auditTable_[1]);
    logfileLocation = clusterManagementJob.metadataHelper.getTableLocation(table);
    logfileName = clusterManagementJob.applicationID;
  }

  public void writeAuditLine(String action, String descriptor, String message, boolean isSuccess)
      throws IOException {
    String className = Thread.currentThread().getStackTrace()[2].getClassName();
    String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
    String payload = String.format("%s~%s~%s~%s~%s~%s~\"%s\"~%s~%s\n"
        , className
        , methodName
        , clusterManagementJob.applicationID
        , clusterManagementJob.trackingURL
        , action
        , descriptor
        , message
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        , isSuccess
    );

    SparkHelper.Hdfs.appendFileContent(logfileLocation, logfileName, payload);

//        TODO - Reinstate above line, remove below   append() not supported for local filesystem
//        Exception in thread "main" java.io.IOException: Not supported
//        at org.apache.hadoop.fs.ChecksumFileSystem.append(ChecksumFileSystem.java:357)
//        File file = new File(logfileLocation.substring(5)+"/"+logfileName);
//        Files.write(file.toPath(), payload.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
  }

  public void startup() throws IOException {
    writeAuditLine(jobType + " - Launch", "", "Process start", true);
  }

  public void completion() throws IOException {
    writeAuditLine(jobType + " - Complete", "", "Process end", true);
  }

}
