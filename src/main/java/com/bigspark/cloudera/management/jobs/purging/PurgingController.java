package com.bigspark.cloudera.management.jobs.purging;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.PurgingMetadata;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.helpers.SparkSqlAuditHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class PurgingController extends ClusterManagementJob {

//  public SparkHelper.AuditedSparkSession_NEW spark;
  public SparkSqlAuditHelper sqlAuditHelper;
  private final String cPURGING_CONFIG_TABLE="SYS_CM_PURGE_CONFIG";
  Logger logger = LoggerFactory.getLogger(getClass());

  public PurgingController(Boolean isDryRun)
      throws Exception {
    this();
    this.isDryRun = isDryRun;
  }

  public PurgingController()
      throws Exception {
    super();
    //this.sqlAuditHelper = new SparkSqlAuditHelper(this, "Purging job","purging.sqlAuditTable");
    //this.spark = new SparkHelper.AuditedSparkSession_NEW(this.spark, sqlAuditHelper);
  }

  @Override
  protected void jobAuditBegin() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void jobAuditEnd() throws IOException {
    throw new NotImplementedException();
  }

  @Override
  protected void jobAuditFail(String error) throws IOException {
    throw new NotImplementedException();
  }


  private String getMetadataTable() {
    return String.format("%s.%s", this.managementDb, cPURGING_CONFIG_TABLE);
  }


  /**
   * Method to pull distinct list of databases in execution group.  If Retention Group is -1 all are
   * returned
   *
   * @return List<Row> A list of database names within the specified retention group
   */
  private List<Row> getGroupDatabases(int group) {
    logger.info("Now pulling list of databases for group : " + group);
    String sql = "SELECT DISTINCT DB_NAME FROM " + this.getMetadataTable() + " WHERE ACTIVE='true'";
    if (group > 0) {
      sql += " and PROCESSING_GROUP=" + group;
    }
    logger.debug("Returning DB Config SQL: " + sql);
    return spark.sql(sql).collectAsList();
  }

  /**
   * Method to pull list of tables within a specific database for purging
   *
   * @param database
   * @param group
   * @return List<Row> List of Records for the given DB/Group
   */
  private List<Row> getRetentionDataForDatabase(String database, int group) {
    this.initialiseConfigTableView();
    logger.info("Now pulling configuration metadata for all tables in database : " + database);
    String sql =
        "SELECT DISTINCT TBL_NAME, RETENTION_PERIOD, RETAIN_MONTH_END FROM " + this.getMetadataTable()
            + " WHERE DB_NAME = '" + database + "' AND LOWER(ACTIVE)='true'";
    if (group >= 0) {
      sql += " AND PROCESSING_GROUP =" + group;
    }
    logger.debug("Returning DB Table Config SQL: " + sql);
    return spark.sql(sql).collectAsList();
  }

  private void initialiseConfigTableView()  {
    String viewName = String.format("%s_CURRENT_V ", this.getMetadataTable());
    if(!this.spark.catalog().tableExists(viewName)) {
      logger.info(String.format("Job Audit table view does not exist.  Creating view %s "
          , viewName));
      spark.sql(this.getConfigTableViewSql(viewName));
    }
  }

  private String getConfigTableViewSql(String viewName) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("CREATE VIEW IF NOT EXISTS %s AS\n", viewName));
    sql.append("select pc.*, nvl(a.status, 'NOT_RUN') as status, a.log_time\n");
    sql.append("from bddlsold01p.SYS_CM_PURGE_CONFIG pc\n");
    sql.append("left outer join bddlsold01p.SYS_CM_JOB_AUDIT_CURRENT_V a\n");
    sql.append("on pc.db_name=a.database_name\n");
    sql.append("and pc.tbl_name=a.table_name\n");
    sql.append("and a.job_type = 'PURGE'\n");
    logger.trace(String.format("Config Table view SQL:\n%s", sql.toString()));
    return sql.toString();
  }

  /**
   * Method to fetch the Purging metadata for a specific database
   *
   * @param database
   * @param group
   * @return RetentionMetadataContainer
   */
  private ArrayList<PurgingMetadata> sourceDatabaseTablesFromMetaTable(String database,
      int group) throws SourceException {
    List<Row> purgeTables = getRetentionDataForDatabase(database, group);
    ArrayList<PurgingMetadata> PurgingMetadataList = new ArrayList<>();
    logger.info(purgeTables.size() + " tables returned with a purge configuration");
    for (Row table : purgeTables) {
      String tableName = table.get(0).toString();
      Integer retentionPeriod = (Integer) table.get(1);
      boolean isRetainMonthEnd = Boolean.parseBoolean(String.valueOf(table.get(2)));
      try {
        logger.debug(String.format("Getting metadata for Table %s.%s", database, tableName));
        Table tableMeta = metadataHelper.getTable(database, tableName);
        TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
        PurgingMetadata PurgingMetadata = new PurgingMetadata(database, tableName,
            retentionPeriod, isRetainMonthEnd, tableDescriptor);
        PurgingMetadataList.add(PurgingMetadata);
      } catch (SourceException e) {
        logger
            .error(tableName + " : provided in metadata configuration, but not found in database..",
                e);
      }
    }
    return PurgingMetadataList;
  }


  public void execute() throws Exception {
    List<Row> retentionGroup = getGroupDatabases(-1);
    this.execute(retentionGroup, -1);
  }

  public void execute(int executionGroup)
      throws Exception {
    List<Row> retentionGroup = getGroupDatabases(executionGroup);
    this.execute(retentionGroup, executionGroup);
  }

  public void execute(List<Row> databases, int executionGroup)
      throws Exception {

    databases.forEach(retentionRecord -> {
      String database = retentionRecord.get(0).toString();
      logger.info(String
          .format("Running Purging for database '%s' and processing group : %s ", database,
              executionGroup));
      ArrayList<PurgingMetadata> PurgingMetadataList = new ArrayList<>();
      try {
        PurgingMetadataList
            .addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
      } catch (SourceException e) {
        e.printStackTrace();
      }
      PurgingMetadataList.forEach(table -> {
        try {
          logger.info(
              String.format("Running purging for table '%s.%s'", database, table.tableName));
          PurgingJob PurgingJob = new PurgingJob(this, table);
          PurgingJob.execute();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
      this.impalaInvalidateMetadata(this.managementDb, this.cJOB_AUDIT_TABLE_NAME);
    });
  }
}

