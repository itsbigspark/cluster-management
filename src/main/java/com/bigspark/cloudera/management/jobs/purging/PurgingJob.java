package com.bigspark.cloudera.management.jobs.purging;


import com.bigspark.cloudera.management.common.enums.JobType;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.PurgingMetadata;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.utils.DateUtils;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.GenericAuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.ClusterManagementTableJob;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * purging job Used to purge files from HDFS/Hive based on a provided table retention
 * parameter
 *
 * @author Chris Finlayson
 */
public class PurgingJob extends ClusterManagementTableJob {

  protected final String cPURGING_JOB_AUDIT_TABLE_NAME="SYS_CM_PURGE_AUDIT";
  protected final GenericAuditHelper purgingJobPartitionAudit;
  public SourceDescriptor sourceDescriptor;
  PurgingMetadata purgingMetadata;
  Pattern pattern;
  Dataset<Row> partitionMonthEnds;

  Logger logger = LoggerFactory.getLogger(getClass());

  public PurgingJob(ClusterManagementJob existing, PurgingMetadata purgingMetadata) throws Exception {
    super(existing, purgingMetadata.database, purgingMetadata.tableName);
    this.purgingMetadata = purgingMetadata;
    this.initialisePurgingJobAuditTable();
    this.purgingJobPartitionAudit = new GenericAuditHelper(
        this
        , this.purgingMetadata.database
        , this.cPURGING_JOB_AUDIT_TABLE_NAME);
  }

  protected void getTablePartitionMonthEnds(String dbName, String tableName,
      List<Partition> partitions, LocalDate purgeCeiling) {
    StringBuilder sb = new StringBuilder();
    LocalDate maxMonthMinBusinessDay = this.metadataHelper.getMaxBusinessDay(partitions);
    logger.debug(String
        .format("Getting Month End data  >= '%s' which is the current max month end date",
            maxMonthMinBusinessDay));
    if (pattern == Pattern.EAS) {
      sb.append(String.format("SELECT" +
              " SRC_SYS_ID" +
              ", SRC_SYS_INST_ID" +
              ", to_date(year(EDI_BUSINESS_DAY)" +
              "||'-'||lpad(month(EDI_BUSINESS_DAY),2,'0')" +
              "||'-'||lpad(MAX(day(EDI_BUSINESS_DAY)),2,'0')" +
              ") AS MONTH_END" +
              " FROM %s.%s " +
              "WHERE EDI_BUSINESS_DAY > '" + maxMonthMinBusinessDay.toString() + "' " +
              "AND EDI_BUSINESS_DAY < '" + purgeCeiling.toString() + "' " +
              "GROUP BY SRC_SYS_ID, SRC_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
          , dbName, tableName));
      this.partitionMonthEnds = spark.sql(sb.toString()).cache();
      List<String> dates = new ArrayList<>();
      this.partitionMonthEnds.collectAsList().forEach(row -> {
        logger.debug(String.format(
            "Retained EAS Month End Partition: edi_business_day=%s, src_sys_id=%s,src_sys_inst_id=%s"
            , row.getDate(row.fieldIndex("MONTH_END")).toString()
            , row.getDate(row.fieldIndex("SRC_SYS_ID")).toString()
            , row.getDate(row.fieldIndex("SRC_SYS_INST_ID")).toString()
        ));
      });
    } else if (pattern == Pattern.SH) {
      sb.append(String.format("SELECT" +
              " SRC_SYS_INST_ID" +
              ", to_date(year(EDI_BUSINESS_DAY)" +
              "||'-'||lpad(month(EDI_BUSINESS_DAY),2,'0')" +
              "||'-'||lpad(MAX(day(EDI_BUSINESS_DAY)),2,'0')" +
              ") AS MONTH_END" +
              " FROM %s.%s " +
              "WHERE EDI_BUSINESS_DAY > '" + maxMonthMinBusinessDay.toString() + "' " +
              "AND EDI_BUSINESS_DAY < '" + purgeCeiling.toString() + "' " +
              "AND to_date(EDI_BUSINESS_DAY) IS NOT NULL " +
              "GROUP BY SRC_SYS_INST_ID, year(EDI_BUSINESS_DAY), month(EDI_BUSINESS_DAY)"
          , dbName, tableName));
      logger.trace("Month End SQL: " + sb.toString());
      this.partitionMonthEnds = spark.sql(sb.toString()).cache();
      List<String> dates = new ArrayList<>();
      this.partitionMonthEnds.collectAsList().forEach(row -> {
        logger.debug(String.format("Retained SH Month End Partition: edi_business_day=%s"
            , row.getDate(row.fieldIndex("MONTH_END")).toString()));
      });
    }
    if (this.partitionMonthEnds.count() > 0) {
      this.partitionMonthEnds.show();
    } else {
      logger.info("There are no month ends to preserve");
    }
  }


  /**
   * Method to calculate the minimum retention date (purge ceiling) All dates before that value
   * should be included into purge scope.
   *
   * @param retentionPeriod
   * @param currentDate
   * @return LocalDate
   */
  protected LocalDate calculatePurgeCeiling(int retentionPeriod, LocalDate currentDate,
      Boolean retainMonthEnds) {
    logger.debug("Retention period : " + retentionPeriod);
    if (retentionPeriod <= 0) {
      logger
          .warn("Invalid retention period [ " + retentionPeriod + "]. Returning zero partitions.");
      return currentDate;
    }
    LocalDate purgeCeiling = currentDate.minusDays(retentionPeriod);
    logger.debug(String.format("purge ceiling calculated as '%s'", purgeCeiling));
    if (retainMonthEnds) {
      purgeCeiling = DateUtils.getMonthStart(purgeCeiling);
      logger.debug(String
          .format("purge ceiling re-calculated for month end retention as '%s'", purgeCeiling));
    }
    return purgeCeiling;
  }

  /**
   * Method to drop table partition using HiveMetaStoreClient
   *
   * @param partition
   * @throws TException
   */
  void dropHivePartition(Partition partition) throws TException {
    if (getDryRun()) {
      logger.info("DRY RUN - Dropped partition : " + partition.getValues().toString());
    } else {
      MetadataHelper.dropHivePartition(partition,this.hiveMetaStoreClient);
      logger.debug("Dropped partition : " + partition.getValues().toString());
    }
  }

  protected List<Partition> removePartitionMonthEnds(List<Partition> partitionList) {
    ArrayList<Partition> eligiblePartitions = new ArrayList<>();
    LocalDate maxEdiBusinessDay = this.metadataHelper.getMaxBusinessDay(partitionList);
    partitionList.forEach(
        partition -> {
          LocalDate partitionDate = LocalDate.parse(partition.getValues().get(0));
          Boolean isMonthEnd = this.metadataHelper.isMonthEnd(partition);
          if (!isMonthEnd) {
            eligiblePartitions.add(partition);
            logger.trace(
                String.format("Partition '%s' added as it is not currently marked as a month end'", partitionDate));
          } else {
            logger.trace(
                String.format("Partition '%s' excluded as it is marked as a month end'", partitionDate));
          }
        }
    );
    return eligiblePartitions;
  }


  protected void createMonthEndSwingTable(String database, String table) {
    if ((this.partitionMonthEnds.count() > 0)) {
      logger.debug("Creating swing table for month end partitions");
      this.partitionMonthEnds.createOrReplaceTempView("partitionMonthEnds");
      if (this.pattern == Pattern.SH) {
        this.spark.sql(String.format("DROP TABLE IF EXISTS %s.%s_swing", database, table));
        this.spark.sql(String.format(
            "CREATE TABLE %s.%s_swing AS SELECT t.* from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SRC_SYS_INST_ID = me.SRC_SYS_INST_ID",
            database, table, database, table));
      } else if (pattern == Pattern.EAS) {
        this.spark.sql(String.format("DROP TABLE IF EXISTS %s.%s_swing", database, table));
        this.spark.sql(String.format(
            "CREATE TABLE %s.%s_swing AS SELECT t.* from %s.%s t join partitionMonthEnds me on t.EDI_BUSINESS_DAY = me.MONTH_END and t.SRC_SYS_ID = me.SRC_SYS_ID and t.SRC_SYS_INST_ID = me.SRC_SYS_INST_ID",
            database, table, database, table));
      }
    } else {
      logger.debug("Skipping Creating swing table as no month ends");
    }
  }

  protected void trashDataOutwithRetention(List<Partition> purgeCandidates)
      throws IOException, URISyntaxException {
    if (purgeCandidates.size() >= 0) {
      String trashBaseLocation = FileSystemHelper.getCreateTrashBaseLocation("purging"
          , this.purgingMetadata.database
          , this.purgingMetadata.tableName);

      for (Partition p : purgeCandidates) {
        URI partitionLocation = new URI(p.getSd().getLocation());
        FileSystemHelper
            .moveDataToUserTrashLocation(partitionLocation.getPath(), trashBaseLocation,
                getDryRun(),
                fileSystem);
        String payload = this.getPurgingAuditLogRecord(
            this.purgingMetadata.database
            , this.purgingMetadata.tableName
            , partitionLocation.getPath()
            , trashBaseLocation);

        this.purgingJobPartitionAudit.write(payload);
      }
      this.cleanUpPartitions(purgeCandidates);
    }
  }

  protected void cleanUpPartitions(List<Partition> purgeCandidates) {
    purgeCandidates.forEach(p -> {
      try {
        this.dropHivePartition(p);
      } catch (TException e) {
        e.printStackTrace();
      }
    });
  }

  protected void reinstateMonthEndPartitions(String database, String table) {
    if (this.partitionMonthEnds.count() > 0) {
      logger.debug("Resolving source table by reinstating month end partitions");
      Dataset<Row> swingTable = this.spark
          .table(String.format("%s.%s_swing", database, table));
      if (pattern == Pattern.SH) {
//            swingTable.write().partitionBy("EDI_BUSINESS_DAY").insertInto(String.format("%s.%s", database, table));
        swingTable.coalesce(1).write().insertInto(String.format("%s.%s", database, table));
//            insertInto() can't be used together with partitionBy()
//            Partition columns have already been defined for the table.
//            It is not necessary to use partitionBy().
      } else if (pattern == Pattern.EAS) {
//               swingTable.write().partitionBy("EDI_BUSINESS_DAY,SOURCE_SYS_ID,SOURCE_SYS_INST_ID").insertInto(String.format("%s.%s", database, table));
        swingTable.write().insertInto(String.format("%s.%s", database, table));
      }
    } else {
      logger.debug("There are no Month End Partitions to reinstate");
    }
  }

  protected void manageMonthEndPartitionMetadata(String database, String table)
      throws TException, ParseException {

    List<Row> partitions = this.partitionMonthEnds.collectAsList();

    //Loop 1 - Mark Partitions as month_end:true in the  Hive Metastore
    for (Row partition : partitions) {
      String edi_business_day = DateUtils.getFormattedDate(partition.getDate(1), "yyyy-MM-dd");
      logger.debug(String
          .format("Marking partition '%s' as month end for table %s.%s", edi_business_day, database,
              table));
      Partition p = this.hiveMetaStoreClient
          .getPartition(database, table, String.format("edi_business_day=%s", edi_business_day));

      p.getParameters().put("month_end", "true");
      this.hiveMetaStoreClient.alter_partition(database, table, p);
    }

    //Loop 2 - compute stats for impala - NOTE this cannot be done in loop 1 or it
    //wipes out the month_end parameter unless we invalidate metadata each time!
    this.impalaInvalidateMetadata(database, table);
    for (Row partition : partitions) {
      String edi_business_day = DateUtils.getFormattedDate(partition.getDate(1), "yyyy-MM-dd");
      String partitionSpec = String.format("edi_business_day ='%s'", edi_business_day);
      logger.debug(String.format("Computing stats for %s.%s (%s)", database, table, partitionSpec));
      this.impalaComputeStats(database, table, partitionSpec);
    }


    this.partitionMonthEnds.unpersist();
  }

  /**
   * Method to delete a HDFS location using filesystem API
   *
   * @param partition
   * @throws IOException
   */
  protected void purgeHDFSPartition(Partition partition) throws IOException {
    boolean delete = fileSystem
        .delete(new Path(metadataHelper.getPartitionLocation(partition)), true);
    if (!delete) {
      throw new IOException(
          "Unexpected error deleting location: " + metadataHelper.getPartitionLocation(partition));
    }
  }



  /**
   * Main entry point method for executing the purging process
   *
   * @throws MetaException
   * @throws SourceException
   */
  void execute()
      throws SourceException, IOException, URISyntaxException, TException, ParseException {
    this.jobAuditBegin();
    this.sourceDescriptor = new SourceDescriptor(
        metadataHelper.getDatabase(purgingMetadata.database)
        , purgingMetadata.tableDescriptor);

    if (purgingMetadata.tableDescriptor.hasPartitions()) {
      this.pattern = MetadataHelper.getTableType(purgingMetadata.tableDescriptor);
      if (this.pattern != null && this.pattern != Pattern.UNKNOWN) {
        LocalDate purgeCeiling = this.calculatePurgeCeiling(
            purgingMetadata.retentionPeriod
            , LocalDate.now()
            , purgingMetadata.isRetainMonthEnd);
        
        List<Partition> allPurgeCandidates = MetadataHelper.removePartitionToRetain(
            purgingMetadata.tableDescriptor.getPartitionList()
            , purgeCeiling);

        logger.debug("Partitions returned as eligible for purge : " + allPurgeCandidates.size());

        if (purgingMetadata.isRetainMonthEnd) {
          executeMonthEndPurging(allPurgeCandidates, purgeCeiling);
        } else {
          this.trashDataOutwithRetention(allPurgeCandidates);
          this.impalaInvalidateMetadata(
              purgingMetadata.database,
              purgingMetadata.tableName, allPurgeCandidates);
        }
        this.jobAuditEnd();
        this.purgingJobPartitionAudit.invalidateAuditTableMetadata();
      } else {
        String message = "Table pattern not validated";
        logger.error(message);
        this.jobAuditFail(message);
      }
    } else {
      String message = String.format("Skipping table '%s.%s' as it has no partitions, or has a non-standard partition key  "
          , purgingMetadata.tableDescriptor.getDatabaseName()
          , purgingMetadata.tableDescriptor.getTableName());
      logger.warn(message);
      this.jobAuditFail(message);
    }
    this.impalaInvalidateMetadata(this.managementDb, this.cPURGING_JOB_AUDIT_TABLE_NAME);
  }

  private void executeMonthEndPurging(List<Partition> allPurgeCandidates,
      LocalDate purgeCeiling)
      throws IOException, URISyntaxException, TException, ParseException {
    logger.debug("RetainMonthEnd config passed from metadata table");


    this.getTablePartitionMonthEnds(
        this.purgingMetadata.database,
        this.purgingMetadata.tableName
        , allPurgeCandidates, purgeCeiling);

    logger.debug("Removing Month End Partitions that have been previously processed");
    allPurgeCandidates = this.removePartitionMonthEnds(allPurgeCandidates);


    this.createMonthEndSwingTable(
        this.purgingMetadata.database,
        this.purgingMetadata.tableName);

    this.trashDataOutwithRetention(allPurgeCandidates);

    this.reinstateMonthEndPartitions(
        this.purgingMetadata.database,
        this.purgingMetadata.tableName);

    //Mark new Month End partitions as month_end:true and compute stats
    this.manageMonthEndPartitionMetadata(
        purgingMetadata.database,
        purgingMetadata.tableName);

    //Drop the swing table
    this.dropMonthEndSwingTable();
  }

  private void dropMonthEndSwingTable() {
    try {
      this.impalaHelper.execute(
          String.format("drop table if exists %s.%s_swing", this.databaseName, this.tableName));
    } catch(Exception ex) {
      logger.warn(String.format("Could not drop month end swing table %s.%s"
          , this.databaseName
          , this.tableName), ex);
    }
  }

  private String getPurgingAuditLogRecord(String dbName, String tableName, String originalLocation,
      String trashLocation) {
    return String.format("%s~%s~%s~%s~%s~%s~%s\n"
        , this.applicationID
        , dbName
        , tableName
        , "partition_spec"
        , originalLocation
        , trashLocation
        , LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    );
  }

  public Boolean getDryRun() {
    return isDryRun;
  }

  public void setDryRun(Boolean dryRun) {
    isDryRun = dryRun;
  }

  private void initialisePurgingJobAuditTable() throws TException {
    if(!this.hiveMetaStoreClient.tableExists(this.managementDb, this.cPURGING_JOB_AUDIT_TABLE_NAME)) {
      logger.info(String.format("Purging Job Audit table does not exist.  Creating %s.%s"
          , this.managementDb
          , this.cPURGING_JOB_AUDIT_TABLE_NAME));
      spark.sql(this.getPurgingJobAuditTableSql());
    }
  }

  private String getPurgingJobAuditTableSql() {
    StringBuilder sql  = new StringBuilder();
    sql.append(String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (\n", this.managementDb, this.cPURGING_JOB_AUDIT_TABLE_NAME));
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
          , this.cPURGING_JOB_AUDIT_TABLE_NAME));
    }
    return sql.toString();
  }

  @Override
  public JobType getJobType() {
    return JobType.PURGE;
  }
}
