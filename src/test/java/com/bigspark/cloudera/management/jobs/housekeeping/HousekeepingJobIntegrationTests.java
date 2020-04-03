package com.bigspark.cloudera.management.jobs.housekeeping;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import com.bigspark.cloudera.management.jobs.TstDataSetup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HousekeepingJobIntegrationTests {

    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public HousekeepingController housekeepingController;
    public String testingDatabase;
    public String testFile;
    private String metatable;
    public AuditHelper auditHelper;
    Logger logger = LoggerFactory.getLogger(getClass());

    public HousekeepingJobIntegrationTests() throws IOException, MetaException, ConfigurationException, SourceException {

        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.housekeepingController = new HousekeepingController();
        this.auditHelper = new AuditHelper(clusterManagementJob, "EDH Cluster housekeeping test");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }


    private void checkHousekeepingResult(String database, String table, String sys, String inst, int numOffsets, Pattern pattern, int retentionDays){
        if (pattern == Pattern.EAS) {
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where src_sys_id='%s' and src_sys_inst_id='%s'", database, table, sys, inst)).count();
            assertEquals(count,(numOffsets - retentionDays));
        } else if (pattern == Pattern.SH){
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where src_sys_inst_id='%s'", database, table, inst)).count();
            assertEquals(count,(numOffsets - retentionDays));
        }
    }

    public void createMetadataTable(){
        if (!spark.catalog().tableExists(testingDatabase, metatable.split("\\.")[0])) {
            System.out.println("Creating test metadata table...");
            spark.sql("CREATE TABLE IF NOT EXISTS "+metatable+" (database STRING, table STRING, retention_period INT, retain_month_end STRING, group INT, active STRING)");
            spark.sql("INSERT INTO "+metatable+" VALUES " +
                    "('"+testingDatabase+"','test_table_sh',10,'true',1,'true')," +
                    "('"+testingDatabase+"','test_table_eas',10,'true',2,'true')");
            spark.sql("SHOW TABLES").show();
        }
    }


    void execute() throws ConfigurationException, IOException, MetaException, ParseException, SourceException {

        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.testingDatabase");
        this.metatable = jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.metatable");
        TstDataSetup tstDataSetup = new TstDataSetup();
        tstDataSetup.setUp(testingDatabase, metatable);
        createMetadataTable();

        System.out.print(Common.getBannerStart("Execution group 1 - SH"));
        housekeepingController.executeHousekeepingGroup(1);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","NWB",100,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","UBR",90,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","UBN",80,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","RBS",70,Pattern.SH,100);
        System.out.print(Common.getBannerFinish("Execution group 1 - SH"));

        Common.getBannerStart("Execution group 2 - EAS");
        housekeepingController.executeHousekeepingGroup(2);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","NWB",100,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","UBR",90,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","UBN",80,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","RBS",70,Pattern.EAS,100);
        System.out.print(Common.getBannerFinish("Execution group 2 - EAS"));

    }

}
