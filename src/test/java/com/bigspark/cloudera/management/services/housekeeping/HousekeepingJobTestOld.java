package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.*;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import scala.math.Ordering;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HousekeepingJobTestOld {

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

    public HousekeepingJobTestOld() throws IOException, MetaException, ConfigurationException {
        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.housekeepingController = new HousekeepingController();
        this.spark = clusterManagementJob.spark;
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    void setUp() throws ConfigurationException, IOException, MetaException {
        FileUtils.deleteDirectory(new File("/opt/spark/2.4.3/metastore_db")); //Required due to odd locking issues
//        FileUtils.deleteDirectory(new File("/opt/spark/2.4.3/spark-warehouse/housekeeping_testing.db/"));


//        if (!spark.catalog().tableExists("housekeeping_testing", "data_retention_configuration"))
//            sb.append("CREATE TABLE housekeeping_testing.data_retention_configuration (database STRING, table STRING, retention_period INT, retain_month_end STRING, group INT); \n");
//
//        sb.append("INSERT INTO housekeeping_testing.data_retention_configuration VALUES " +
//                "('housekeeping_testing','testTableSH',30,'false',1)" +
//                ",('housekeeping_testing','testTableSH2',40,'false',2)" +
//                ",('housekeeping_testing','testTableEAS',30,'false',2)" +
//                ",('housekeeping_testing','testTableEAS2',40,'false',2) ; \n");
//
//        if (!spark.catalog().databaseExists("housekeeping_testing"))
//            sb.append("CREATE DATABASE IF NOT EXISTS housekeeping_testing; \n");

//        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.testTableSH");
//        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.testTableEAS");
//        spark.sql("DROP TABLE IF EXISTS housekeeping_testing.data_retention_configuration");
//
//        File testFileSH = new File("/tmp/testdataSH.csv");
//
//        if (! testFileSH.exists()) {
//            StringBuilder sb = new StringBuilder();
//
//            createTable("housekeeping_testing", "testTableSH", Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH", "ADB", "NWB", 100, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH", "ADB", "UBR", 90, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH", "ADB", "UBN", 80, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH", "ADB", "RBS", 70, Pattern.SH, sb);
//
//            createTable("housekeeping_testing", "testTableSH2", Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH2", "ADB", "NWB", 100, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH2", "ADB", "UBR", 90, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH2", "ADB", "UBN", 80, Pattern.SH, sb);
//            generateTestData("housekeeping_testing", "testTableSH2", "ADB", "RBS", 70, Pattern.SH, sb);
//
//            System.out.println(sb.toString());
//
//            BufferedWriter writer = null;
//            try {
//                writer = new BufferedWriter(new FileWriter(testFileSH));
//                writer.write(sb.toString());
//            } finally {
//                if (writer != null) writer.close();
//            }
//        }

        File testFileEAS = new File("/tmp/testdata.csv");

        if (! testFileEAS.exists()) {
            StringBuilder sb = new StringBuilder();

            generateTestData("ADB", "NWB", 10000, Pattern.EAS, sb);
            generateTestData("ADB", "UBR", 90000, Pattern.EAS, sb);
            generateTestData("ADB", "UBN", 80000, Pattern.EAS, sb);
            generateTestData("ADB", "RBS", 70000, Pattern.EAS, sb);

            generateTestData("ADB", "NWB", 10000, Pattern.EAS, sb);
            generateTestData("ADB", "UBR", 90000, Pattern.EAS, sb);
            generateTestData("ADB", "UBN", 80000, Pattern.EAS, sb);
            generateTestData("ADB", "RBS", 70000, Pattern.EAS, sb);

            System.out.println(sb.toString());

            BufferedWriter writer = null;
            try {
                writer = new BufferedWriter(new FileWriter(testFileEAS));
                writer.write(sb.toString());
            } finally {
                if (writer != null) writer.close();
            }
        }
    }

    private String generateBusinessDate(int offset){
//        Dataset<Row> df = spark.sql("SELECT year(now())||'-'||format_string('%02d',month(now()))||'-'||format_string('%02d',day(now())-" + offset + ")");
//        Dataset<Row> df = spark.sql("SELECT date_add(now(),-"+offset+")");
//        return df.as(Encoders.STRING()).collectAsList().get(0);
        return LocalDate.now().minusDays(offset).toString();
    }

    private void createTable(String database, String table, Pattern pattern, StringBuilder sb){
        try {
            if (pattern == Pattern.SH) {
                spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s.%s (value STRING, source_sys_id STRING, source_sys_inst_id STRING) PARTITIONED BY (edi_business_day TIMESTAMP)", database, table));
            } else if (pattern == Pattern.EAS) {
                spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s.%s (value STRING) PARTITIONED BY (edi_business_day TIMESTAMP, source_sys_id STRING, source_sys_inst_id STRING)", database, table));
            }
        } catch (Exception e){
        }
    }

    private StringBuilder addPartitionAndData(String database, String table, String sys, String inst, int offset, Pattern pattern) {
        StringBuilder sb = new StringBuilder();
        if (pattern == Pattern.EAS) {
            sb.append(String.format("ALTER TABLE %s.%s ADD PARTITION IF NOT EXISTS (edi_business_day='%s',source_sys_id='%s',source_sys_inst_id='%s'); \n", database, table,generateBusinessDate(offset), sys, inst, offset));
            sb.append(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='%s',source_sys_id='%s',source_sys_inst_id='%s') VALUES ('test_%d'); \n", database, table,generateBusinessDate(offset), sys, inst, offset));
        } else if (pattern == Pattern.SH){
            sb.append(String.format("ALTER TABLE %s.%s ADD PARTITION IF NOT EXISTS (edi_business_day='%s'); \n", database, table, generateBusinessDate(offset)));
            sb.append(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='%s') VALUES ('test_%d','%s','%s'); \n", database, table, generateBusinessDate(offset), offset, sys, inst));
        }
        return sb;
    }

    private StringBuilder addData(String sys, String inst, int offset, Pattern pattern) {
        StringBuilder sb = new StringBuilder();
        if (pattern == Pattern.EAS) {
            sb.append(String.format("test_%d,%s,%s,%s\n",offset,generateBusinessDate(offset), sys, inst));
//            sb.append(String.format("ALTER TABLE %s.%s ADD PARTITION IF NOT EXISTS (edi_business_day='%s',source_sys_id='%s',source_sys_inst_id='%s'); \n", database, table,generateBusinessDate(offset), sys, inst, offset));
//            sb.append(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='%s',source_sys_id='%s',source_sys_inst_id='%s') VALUES ('test_%d'); \n", database, table,generateBusinessDate(offset), sys, inst, offset));
        } else if (pattern == Pattern.SH){
            sb.append(String.format("test_%d,%s,%s,%s\n",offset,generateBusinessDate(offset), sys, inst));
//            sb.append(String.format("ALTER TABLE %s.%s ADD PARTITION IF NOT EXISTS (edi_business_day='%s'); \n", database, table, generateBusinessDate(offset)));
//            sb.append(String.format("INSERT INTO %s.%s PARTITION (edi_business_day='%s') VALUES ('test_%d','%s','%s'); \n", database, table, generateBusinessDate(offset), offset, sys, inst));
        }
        return sb;
    }

    private StringBuilder generateTestData(String sys, String inst, int numOffsets, Pattern pattern, StringBuilder sb){
        sb.append("value,edi_business_day,src_sys_id,src_sys_inst_id");
        IntStream.range(0, numOffsets).forEach(i ->
                sb.append(addData(sys,inst,i,pattern)).toString()
        );
        return sb;
    }

    private void createTables(){
        Dataset<Row> csv = spark.read().option("header","true").csv("/tmp/testdata.csv");
        csv.write().partitionBy("edi_business_day").saveAsTable("testSH");
        csv.write().partitionBy("edi_business_day, src_sys_id, src_sys_inst_id").saveAsTable("testEAS");

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
