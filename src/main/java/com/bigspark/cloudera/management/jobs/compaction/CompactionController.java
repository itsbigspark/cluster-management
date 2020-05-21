package com.bigspark.cloudera.management.jobs.compaction;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.CompactionMetadata;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.naming.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionController extends ClusterManagementJob {

  private final String cCOMPACTION_CONFIG_TABLE = "SYS_CM_COMPACTION_CONFIG";
  Logger logger = LoggerFactory.getLogger(getClass());

  public CompactionController ()
      throws Exception {
    super();
  }

  @Override
  protected void jobAuditBegin() throws IOException {

  }

  @Override
  protected void jobAuditEnd() throws IOException {

  }

  @Override
  protected void jobAuditFail(String error) throws IOException {

  }

  /**
   * Method to fetch metadata table value from properties file
   *
   * @return String
   */
  private String getCompactionTable() {
    //
    return String.format("%s.%s", this.managementDb, cCOMPACTION_CONFIG_TABLE);
  }

  /**
   * Method to pull distinct list of databases for purging scoping
   *
   * @return List<Row>
   */
  private List<Row> getRetentionDatabases() {
    return this.getCompactionGroupDatabases(-1);
  }

  /**
   * Method to pull distinct list of databases in execution group
   *
   * @return List<Row>
   */
  private List<Row> getCompactionGroupDatabases(int group) {
    logger.info("Now pulling list of databases for group : " + group);
    String sql = "SELECT DISTINCT DB_NAME FROM " + this.getCompactionTable() + " WHERE ACTIVE='true'";
    if (group > 0) {
      sql += " and PROCESSING_GROUP=" + group;
    }
    logger.debug("Returning DB Config SQL: " + sql);
    return spark.sql(sql).collectAsList();
  }

  /**
   * Method to pull list of tables in a specific database for purging
   *
   * @return List<Row>
   */
  private List<Row> getCompactionDataForDatabase(String database, int group) {
    this.initialiseConfigTableView();
    StringBuilder sql = new StringBuilder();
    logger.info("Now pulling configuration metadata for all tables in database : " + database);

    sql.append("SELECT DISTINCT TBL_NAME, RETENTION_PERIOD, RETAIN_MONTH_END\n");
    sql.append(String.format ("FROM %s_CURRENT_V\n", this.getCompactionTable()));
    sql.append(String.format("WHERE DB_NAME = '%s'\n",database));
    sql.append(String.format("AND LOWER(ACTIVE)='true'\n"));
    sql.append("AND status in ('FINISHED', 'FIXED', 'NOT_RUN')\n");
    if (group >= 0) {
      sql.append(String.format("AND PROCESSING_GROUP = %d", group));
    }
    logger.debug("Returning DB Table Config SQL:\n" + sql.toString());
    return spark.sql(sql.toString()).collectAsList();
  }


  /**
   * Method to fetch the purging metadata for a specific database
   * Fetch all database tables where * provided in metadata
   * @param database
   * @return RetentionMetadataContainer
   */
  private ArrayList<CompactionMetadata> sourceDatabaseTablesFromMetaTable(String database,
      int group) throws SourceException {
    List<Row> compactionTables = getCompactionDataForDatabase(database, group);
    ArrayList<CompactionMetadata> compactionMetadataList = new ArrayList<>();
    logger.info(compactionTables.size() + " rows returned with a Compaction configuration");
    for (Row table : compactionTables) {
      String tableName = table.get(0).toString();
      if (tableName.equals("*")){
        ArrayList<Table> allTablesFromDatabase = metadataHelper.getAllTablesFromDatabase(database);
        ArrayList<TableDescriptor> allTableDescriptors = metadataHelper
            .getAllTableDescriptors(allTablesFromDatabase);
        allTableDescriptors.forEach(tableDescriptor -> compactionMetadataList.add(new CompactionMetadata(tableDescriptor)));
      } else {
        try {
          Table tableMeta = metadataHelper.getTable(database, tableName);
          TableDescriptor tableDescriptor = metadataHelper.getTableDescriptor(tableMeta);
          CompactionMetadata compactionMetadata = new CompactionMetadata(tableDescriptor);
          compactionMetadataList.add(compactionMetadata);
        } catch (SourceException e) {
          logger.error(
              tableName + " : provided in metadata configuration, but not found in database..");
        }
      }
    }
    return compactionMetadataList;
  }


  public void executeCompactionForLocation(String location)
      throws SourceException, TException, IOException, ConfigurationException {
  //  CompactionJob compactionJob = new CompactionJob();
  //////  CompactionMetadata compactionMetadata = new CompactionMetadata(new Path(location));
  //  compactionJob.execute(compactionMetadata);
  }

  public void execute() throws MetaException, SourceException, ConfigurationException, IOException {
    execute(-1);
  }
  public void execute(int executionGroup)
      throws ConfigurationException, IOException, MetaException, SourceException {

    //auditHelperOLD.startup();
    List<Row> retentionGroup = getCompactionGroupDatabases(executionGroup);
    retentionGroup.forEach(retentionRecord -> {
      String database = retentionRecord.get(0).toString();
      ArrayList<CompactionMetadata> compactionMetadataList = new ArrayList<>();
      try {
        compactionMetadataList.addAll(sourceDatabaseTablesFromMetaTable(database, executionGroup));
      } catch (SourceException e) {
        e.printStackTrace();
      }
      compactionMetadataList.forEach(table -> {
        try {
          CompactionJob compactionJob = new CompactionJob(this, table);
          compactionJob.execute();
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    //auditHelperOLD.completion();
  }

  private void initialiseConfigTableView() {
    String viewName = String.format("%s_CURRENT_V ", this.getCompactionTable());
    if (!this.spark.catalog().tableExists(viewName  )) {
      logger.info(String.format("Job Audit table view does not exist.  Creating view %s "
          , viewName));
      spark.sql(this.getConfigTableViewSql(viewName));
    }
  }

  private String getConfigTableViewSql(String viewName) {
    StringBuilder sql = new StringBuilder();
    sql.append(String.format("CREATE VIEW IF NOT EXISTS %s AS\n", viewName));
    sql.append("select pc.*, nvl(a.status, 'NOT_RUN') as status, a.log_time\n");
    sql.append(String.format("from %s.SYS_CM_COMPACTION_CONFIG pc\n", this.managementDb));
    sql.append(String.format("left outer join %s.SYS_CM_JOB_AUDIT_CURRENT_V a\n", this.managementDb));
    sql.append("on pc.db_name=a.database_name\n");
    sql.append("and pc.tbl_name=a.table_name\n");
    sql.append("and a.job_type = 'COMPACT'\n");
    logger.trace(String.format("Config Table view SQL:\n%s", sql.toString()));
    return sql.toString();
  }
}

