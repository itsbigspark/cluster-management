package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AuditHelper {

    public ClusterManagementJob clusterManagementJob;
    public SparkHelper.AuditedSparkSession spark;
    public String logfileLocation;
    public String logfileName;
    public String auditTable;

    public AuditHelper(ClusterManagementJob clusterManagementJob) throws ConfigurationException, IOException, MetaException, SourceException {
        this.clusterManagementJob = clusterManagementJob;
        intitialiseAuditTable();
        setLogfile();
    }

    private void intitialiseAuditTable() throws IOException {
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,this);
        auditTable = clusterManagementJob.jobProperties.getProperty("com.bigspark.cloudera.management.services.auditTable");
        String[] auditTable_ = auditTable.split("\\.");
        if (! spark.catalog().tableExists(auditTable_[0],auditTable_[1]))
              spark.sql("CREATE TABLE %s (" +
                      "class_name STRING" +
                      ", method_name STRING" +
                      ", application_id STRING" +
                      ", tracking_url STRING" +
                      ", action STRING" +
                      ", descriptor STRING" +
                      ", message STRING" +
                      ", log_time TIMESTAMP" +
                      ", status STRING" +
                      ") STORED AS TEXTFILE"
              );
    }

    private void setLogfile() throws SourceException {
        String[] auditTable_ = auditTable.split("\\.");
        Table table = clusterManagementJob.metadataHelper.getTable(auditTable_[0], auditTable_[1]);
        logfileLocation = clusterManagementJob.metadataHelper.getTableLocation(table);
        logfileName = clusterManagementJob.applicationID;
    }

    public void writeAuditLine(String action, String descriptor, String message, boolean isSuccess) throws IOException {
        String className = Thread.currentThread().getStackTrace()[2].getClassName();
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        String payload = String.format("%s,%s,%s,%s,%s,%s,\"%s\",%s,%s\n"
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
//        SparkHelper.Hdfs.appendFileContent(logfileLocation,logfileName,payload);
        //TODO - Reinstate above line, remove below   append() not supported for local filesystem
//        Exception in thread "main" java.io.IOException: Not supported
//        at org.apache.hadoop.fs.ChecksumFileSystem.append(ChecksumFileSystem.java:357)
        File file = new File(logfileLocation.substring(5)+"/"+logfileName);
        Files.write(file.toPath(), payload.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public void startup() throws IOException {
        writeAuditLine("Init","","Process start",true);
    }

    public void completion() throws IOException {
        writeAuditLine("End","","Process end",true);
    }

}
