package com.bigspark.cloudera.management.jobs;

import com.bigspark.cloudera.management.common.enums.JobState;
import com.bigspark.cloudera.management.common.enums.JobType;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

public abstract class ClusterManagementTableJob extends ClusterManagementJob {

  public final String databaseName;
  public final String tableName;

  public abstract JobType getJobType();

  protected  ClusterManagementTableJob(String databaseName, String tableName) throws Exception {
    super();
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  protected  ClusterManagementTableJob(ClusterManagementJob existing, String databaseName, String tableName) throws Exception {
    super(existing);
    this.databaseName = databaseName;
    this.tableName = tableName;
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
}
