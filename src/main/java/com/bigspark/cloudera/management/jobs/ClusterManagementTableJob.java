package com.bigspark.cloudera.management.jobs;

import com.bigspark.cloudera.management.common.enums.JobState;
import com.bigspark.cloudera.management.common.enums.JobType;
import com.bigspark.cloudera.management.helpers.GenericAuditHelper;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import org.apache.thrift.TException;

public abstract class ClusterManagementTableJob extends ClusterManagementJob {

  public final String databaseName;
  public final String tableName;
  protected final GenericAuditHelper JobPartitionAudit;
  public abstract JobType getJobType();

  protected  ClusterManagementTableJob(String databaseName, String tableName, String auditTableName) throws Exception {
    super();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.JobPartitionAudit = new GenericAuditHelper(this, this.databaseName, auditTableName);
    this.initialiseJobAuditTable(auditTableName);
  }

  protected  ClusterManagementTableJob(ClusterManagementJob existing, String databaseName, String tableName, String auditTableName) throws Exception {
    super(existing);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.JobPartitionAudit = new GenericAuditHelper(this, this.databaseName, auditTableName);
    this.initialiseJobAuditTable(auditTableName);
  }

  @Override
  protected void jobAuditBegin() throws IOException {
    String auditLine =  String.format("%s~%s~%s~%s~%s~%s~%s~%s~%d\n"
        , this.applicationID
        ,this.getJobType()
        , this.databaseName
        , this.tableName
        , "NA"
        , "NA"
        , JobState.STARTED
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        , LocalDateTime.now().getLong(ChronoField.NANO_OF_SECOND)
    );
    this.jobLog.write(auditLine);

  }

  @Override
  protected void jobAuditEnd() throws IOException {

    String auditLine =  String.format("%s~%s~%s~%s~%s~%s~%s~%s~%d\n"
        , this.applicationID
        ,this.getJobType()
        , this.databaseName
        , this.tableName
        , "NA"
        , "NA"
        , JobState.FINISHED
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        , LocalDateTime.now().getLong(ChronoField.NANO_OF_SECOND)
    );
    this.jobLog.write(auditLine);
  }

  @Override
  protected void jobAuditFail(String error) throws IOException {
    String auditLine =  String.format("%s~%s~%s~%s~%s~%s~%s~%s~%d\n"
        , this.applicationID
        ,this.getJobType()
        , this.databaseName
        , this.tableName
        , "NA"
        , error
        , JobState.ERROR
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
        , LocalDateTime.now().getLong(ChronoField.NANO_OF_SECOND)
    );
    this.jobLog.write(auditLine);
  }

  protected void initialiseJobAuditTable(String auditTableName) throws TException {
    if(!this.hiveMetaStoreClient.tableExists(this.managementDb, auditTableName)) {
      logger.info(String.format("Purging Job Audit table does not exist.  Creating %s.%s"
          , this.managementDb
          , auditTableName));
      spark.sql(this.getJobAuditTableSql(auditTableName));
    }
  }

  protected String getJobAuditTableSql(String auditTableName) {
    StringBuilder sql  = new StringBuilder();
    sql.append(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (\n", this.managementDb, auditTableName));
    sql.append("application_id STRING\n");
    sql.append(",database_name STRING\n");
    sql.append(",table_name STRING\n");
    sql.append(",partition_spec STRING\n");
    sql.append(",original_location STRING\n");
    sql.append(",trash_location STRING\n");
    sql.append(",log_time TIMESTAMP\n");
    sql.append(")\n");
    sql.append(" ROW FORMAT DELIMITED\n");
    sql.append(" FIELDS TERMINATED BY '~'\n");
    if(this.managementDbLocation != null  && !this.managementDbLocation.equals("")) {
      sql.append(String.format("LOCATION '%s/data/%s'\n"
          , this.managementDbLocation
          , auditTableName));
    }
    return sql.toString();
  }

  protected String getJobAuditLogRecord(String originalLocation,
      String trashLocation) {
    return String.format("%s~%s~%s~%s~%s~%s~%s\n"
        , this.applicationID
        , this.databaseName
        , this.tableName
        , "partition_spec"
        , originalLocation
        , trashLocation
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    );
  }
}
