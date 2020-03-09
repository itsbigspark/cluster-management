package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HousekeepingJobTest {

//    spark.sql("CREATE TABLE housekeeping_testing.testTableSH (value STRING, source_sys_id STRING, source_sys_inst_id STRING) PARTITIONED BY (edi_business_day timestamp)");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableSH ADD PARTITION (edi_business_day='2020-01-20')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBR')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBR')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBR')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-24') VALUES ('test1','ADB','UBR')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-20') VALUES ('test1','ADB','UBN')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-22') VALUES ('test1','ADB','UBN')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-23') VALUES ('test1','ADB','UBN')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableSH PARTITION (edi_business_day='2020-01-26') VALUES ('test1','ADB','UBN')");
//
//    spark.sql("CREATE TABLE housekeeping_testing.testTableEAS (value STRING) PARTITIONED BY (edi_business_day timestamp, source_sys_id STRING, source_sys_inst_id STRING)");
//
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(0)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(1)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(2)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(3)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(4)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(5)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(6)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(7)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//    spark.sql("ALTER TABLE housekeeping_testing.testTableEAS ADD PARTITION (edi_business_day='"+ generateBusinessDate(7)+"',source_sys_id='ADB',source_sys_inst_id='UBR')");
//
//
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(0)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(1)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(2)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(3)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(4)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(5)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(6)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(7)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");
//    spark.sql("INSERT INTO housekeeping_testing.testTableEAS PARTITION (edi_business_day='"+ generateBusinessDate(8)+"',source_sys_id='ADB',source_sys_inst_id='UBR') VALUES ('test1')");

    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public HousekeepingController housekeepingController;

    public HousekeepingJobTest() throws IOException, MetaException, ConfigurationException {
        this.housekeepingController = new HousekeepingController();
        ClusterManagementJob clusterManagementJob = housekeepingController.getInstance();
        this.spark = clusterManagementJob.spark;
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    void setUp() throws ConfigurationException, IOException, MetaException {

        spark.sql("CREATE DATABASE housekeeping_testing");
        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.testTableSH");
        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.testTableEAS");
        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.data_retention_configuration");
        
        spark.sql("CREATE TABLE housekeeping_testing.data_retention_configuration (database STRING, table STRING, retention_period INT, retain_month_end STRING, group INT)");
        spark.sql("INSERT INTO housekeeping_testing.data_retention_configuration VALUES ('housekeeping_testing','testTableSH',30,'false',1)");
        spark.sql("INSERT INTO housekeeping_testing.data_retention_configuration VALUES ('housekeeping_testing','testTableSH2',40,'false',2)");
        spark.sql("INSERT INTO housekeeping_testing.data_retention_configuration VALUES ('housekeeping_testing','testTableEAS',30,'false',1)");
        spark.sql("INSERT INTO housekeeping_testing.data_retention_configuration VALUES ('housekeeping_testing','testTableEAS2',40,'false',2)");

        createTable("housekeeping_testing","testTableSH",Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH","ADB","NWB",100,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH","ADB","UBR",90,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH","ADB","UBN",80,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH","ADB","RBS",70,Pattern.SH);

        createTable("housekeeping_testing","testTableSH2",Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH2","ADB","NWB",100,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH2","ADB","UBR",90,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH2","ADB","UBN",80,Pattern.SH);
        generateTestData("housekeeping_testing","testTableSH2","ADB","RBS",70,Pattern.SH);

        createTable("housekeeping_testing","testTableEAS",Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS","ADB","NWB",100,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS","ADB","UBR",90,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS","ADB","UBN",80,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS","ADB","RBS",70,Pattern.EAS);

        createTable("housekeeping_testing","testTableEAS2",Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS2","ADB","NWB",100,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS2","ADB","UBR",90,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS2","ADB","UBN",80,Pattern.EAS);
        generateTestData("housekeeping_testing","testTableEAS2","ADB","RBS",70,Pattern.EAS);

    }

    private String generateBusinessDate(int offset){
        return "year(now())||'-'||format_string('%02d',month(now()))||'-'||format_string('%02d',day(now())-"+offset+")";
    }

    private void createTable(String database, String table, Pattern pattern){
        if (pattern == Pattern.SH) {
            spark.sql(String.format("CREATE TABLE %s.%s (value STRING, source_sys_id STRING, source_sys_inst_id STRING) PARTITIONED BY (edi_business_day TIMESTAMP)",database, table));
        } else if (pattern == Pattern.EAS){
            spark.sql(String.format("CREATE TABLE %s.%s (value STRING) PARTITIONED BY (edi_business_day TIMESTAMP, source_sys_id STRING, source_sys_inst_id STRING)",database, table));
        }
    }

    private void addPartitionAndData(String database, String table, String sys, String inst, int offset, Pattern pattern) {
        if (pattern == Pattern.EAS) {
            spark.sql(String.format("ALTER TABLE %s.%s ADD PARTITION (edi_business_day='" + generateBusinessDate(offset) + "',source_sys_id='%s',source_sys_inst_id='%s')", database, table, sys, inst, offset));
            spark.sql(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='" + generateBusinessDate(offset) + "',source_sys_id='%s',source_sys_inst_id='%s') VALUES ('test_%d')", database, table, sys, inst, offset));
        } else if (pattern == Pattern.SH){
            spark.sql(String.format("ALTER TABLE %s.%s ADD PARTITION (edi_business_day='" + generateBusinessDate(offset)+"')", database, table, offset));
            spark.sql(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='" + generateBusinessDate(offset) + "',source_sys_id='%s',source_sys_inst_id='%s') VALUES ('test_%d','%s','%s')", database, table, offset, sys, inst));
        }

    }

    private void generateTestData(String database, String table, String sys, String inst, int numOffsets, Pattern pattern){
        IntStream.range(0, numOffsets).forEach(i ->
                addPartitionAndData(database,table,sys,inst,i,pattern)
        );
    }


    private void checkHousekeepingResult(String database, String table, String sys, String inst, int numOffsets, Pattern pattern, int retentionDays){
        if (pattern == Pattern.EAS) {
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where source_sys_id=%s and source_sys_inst_id=%s", database, table, sys, inst)).count();
            assertEquals(count,(numOffsets - retentionDays));
        } else if (pattern == Pattern.SH){
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where source_sys_inst_id=%s", database, table, inst)).count();
            assertEquals(count,(numOffsets - retentionDays));
        }
    }

    @Test
    void execute() throws ConfigurationException, IOException, MetaException, ParseException {

        setUp();

        Common.getBannerStart("Execution group 1");
        
        housekeepingController.executeHousekeepingGroup(1);

        checkHousekeepingResult("housekeeping_testing","testTableSH","ADB","NWB",100,Pattern.SH,30);
        checkHousekeepingResult("housekeeping_testing","testTableSH","ADB","UBR",90,Pattern.SH,30);
        checkHousekeepingResult("housekeeping_testing","testTableSH","ADB","UBN",80,Pattern.SH,30);
        checkHousekeepingResult("housekeeping_testing","testTableSH","ADB","RBS",70,Pattern.SH,30);

        checkHousekeepingResult("housekeeping_testing","testTableEAS","ADB","NWB",100,Pattern.EAS,30);
        checkHousekeepingResult("housekeeping_testing","testTableEAS","ADB","UBR",90,Pattern.EAS,30);
        checkHousekeepingResult("housekeeping_testing","testTableEAS","ADB","UBN",80,Pattern.EAS,30);
        checkHousekeepingResult("housekeeping_testing","testTableEAS","ADB","RBS",70,Pattern.EAS,30);
        
        Common.getBannerFinish("Execution group 1");

        Common.getBannerStart("Execution group 2");
        housekeepingController.executeHousekeepingGroup(2);
        
        checkHousekeepingResult("housekeeping_testing","testTableSH2","ADB","NWB",100,Pattern.SH,40);
        checkHousekeepingResult("housekeeping_testing","testTableSH2","ADB","UBR",90,Pattern.SH,40);
        checkHousekeepingResult("housekeeping_testing","testTableSH2","ADB","UBN",80,Pattern.SH,40);
        checkHousekeepingResult("housekeeping_testing","testTableSH2","ADB","RBS",70,Pattern.SH,40);
        
        checkHousekeepingResult("housekeeping_testing","testTableEAS2","ADB","NWB",100,Pattern.EAS,40);
        checkHousekeepingResult("housekeeping_testing","testTableEAS2","ADB","UBR",90,Pattern.EAS,40);
        checkHousekeepingResult("housekeeping_testing","testTableEAS2","ADB","UBN",80,Pattern.EAS,40);
        checkHousekeepingResult("housekeeping_testing","testTableEAS2","ADB","RBS",70,Pattern.EAS,40);
        
        Common.getBannerFinish("Execution group 2");
        
    }

}
