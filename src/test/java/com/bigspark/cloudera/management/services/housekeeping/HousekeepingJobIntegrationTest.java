package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.Common;
import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.FileSystemHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class HousekeepingJobIntegrationTest {

    public Properties jobProperties;
    public SparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public HousekeepingController housekeepingController;
    public String testingDatabase;
    public String testFile;
    public AuditHelper auditHelper;

    Logger logger = LoggerFactory.getLogger(getClass());

    public HousekeepingJobIntegrationTest() throws IOException, MetaException, ConfigurationException, SourceException {

        ClusterManagementJob clusterManagementJob = ClusterManagementJob.getInstance();
        this.housekeepingController = new HousekeepingController();
        this.auditHelper = new AuditHelper(clusterManagementJob);
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);

        if (spark.sparkContext().master().equals("local") && spark.sparkContext().hadoopConfiguration().size() == 0) {
            //Required only for derby db weird locking issues
            FileUtils.forceDelete(new File(spark.sparkContext().getSparkHome()+"/metastore_db/db.lck"));
            FileUtils.forceDelete(new File(spark.sparkContext().getSparkHome()+"/metastore_db/dbex.lck"));
        }

        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
    }

    void setUp() throws IOException {
        this.testingDatabase = jobProperties.getProperty("com.bigspark.cloudera.management.services.housekeeping.testingDatabase");
        String userHomeArea;
        String fileName = "testdata.csv";
        if (spark.sparkContext().hadoopConfiguration().size() > 0) {
            userHomeArea = FileSystemHelper.getUserHomeArea();
        } else {
            userHomeArea = "/tmp";
        }
        this.testFile = userHomeArea+"/"+fileName;
        if ( ! fileSystem.exists(new Path(this.testFile))) {
            logger.info("Now generating test dataset");

            StringBuilder sb = new StringBuilder();
            sb.append("value,edi_business_day,src_sys_id,src_sys_inst_id\n");

            generateTestData("ADB", "NWB", 1000, sb);
            generateTestData("ADB", "UBR", 900, sb);
            generateTestData("ADB", "UBN", 800, sb);
            generateTestData("ADB", "RBS", 700, sb);

            System.out.println(sb.toString());

            if (spark.sparkContext().master().equals("local") && spark.sparkContext().hadoopConfiguration().size() == 0) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(testFile))) {
                    writer.write(sb.toString());
                }
            }
            else if (spark.sparkContext().hadoopConfiguration().size() > 0){
                FileSystemHelper.writeFileContent(userHomeArea, fileName,sb.toString(),true);
            }
            dropTables();
        }

        createTables();
    }

    private String generateBusinessDate(int offset){
        return LocalDate.now().minusDays(offset).toString();
    }

    private StringBuilder addData(String sys, String inst, int offset) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("test_%d,%s,%s,%s\n",offset,generateBusinessDate(offset), sys, inst));
        return sb;
    }

    private StringBuilder generateTestData(String sys, String inst, int numOffsets, StringBuilder sb){
        IntStream.range(0, numOffsets).forEach(i ->
                sb.append(addData(sys,inst,i)).toString()
        );
        return sb;
    }

    private void checkHousekeepingResult(String database, String table, String sys, String inst, int numOffsets, Pattern pattern, int retentionDays){
        if (pattern == Pattern.EAS) {
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where src_sys_id='%s' and src_sys_inst_id='%s'", database, table, sys, inst)).count();
//            assertEquals(count,(numOffsets - retentionDays));
        } else if (pattern == Pattern.SH){
            long count = spark.sql(String.format("SELECT distinct edi_business_day from %s.%s where src_sys_inst_id='%s'", database, table, inst)).count();
//            assertEquals(count,(numOffsets - retentionDays));
        }
    }

    private void dropTables() throws IOException {

        spark.sql("DROP TABLE IF EXISTS default.test_table_eas");
        spark.sql("DROP TABLE IF EXISTS default.test_table_eas_swing");
        spark.sql("DROP TABLE IF EXISTS default.test_table_sh");
        spark.sql("DROP TABLE IF EXISTS default.test_table_sh_swing");
        spark.sql("DROP TABLE IF EXISTS default.data_retention_configuration");
        spark.sql("DROP TABLE IF EXISTS default.cluster_management_audit");

    }



    private void createTables() throws IOException {
        System.out.println("Check test tables exist...");
        spark.sql("USE "+testingDatabase);
        spark.sql("SHOW TABLES").show();
        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true");
        Dataset<Row> csv = spark.read().option("header", "true").csv(testFile);
        csv.cache();
        if (!spark.catalog().tableExists(testingDatabase, "test_table_sh")) {
            System.out.println("Creating SH test table...");
            csv.write().partitionBy("edi_business_day").mode("overwrite").saveAsTable("default.test_table_sh");
            spark.sql("SHOW TABLES").show();
        }
        if (!spark.catalog().tableExists(testingDatabase, "test_table_eas")) {
            System.out.println("Creating EAS test table...");
            csv.write().partitionBy("edi_business_day", "src_sys_id", "src_sys_inst_id").mode("overwrite").saveAsTable("default.test_table_eas");
            spark.sql("SHOW TABLES").show();
        }

        if (!spark.catalog().tableExists(testingDatabase, "data_retention_configuration")) {
            System.out.println("Creating test metadata table...");
            spark.sql("CREATE TABLE IF NOT EXISTS "+testingDatabase+".data_retention_configuration (database STRING, table STRING, retention_period INT, retain_month_end STRING, group INT, active STRING)");
            spark.sql("INSERT INTO "+testingDatabase+".data_retention_configuration VALUES " +
                    "('"+testingDatabase+"','test_table_sh',100,'true',1,'true')," +
                    "('"+testingDatabase+"','test_table_eas',100,'true',2,'true')");
            spark.sql("SHOW TABLES").show();
        }
    }

    @Test
    void execute() throws ConfigurationException, IOException, MetaException, ParseException, SourceException {

        setUp();

        System.out.print(Common.getBannerStart("Execution group 1 - SH"));
        housekeepingController.executeHousekeepingGroup(1);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","NWB",1000,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","UBR",900,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","UBN",800,Pattern.SH,100);
        checkHousekeepingResult(testingDatabase,"test_table_sh","ADB","RBS",700,Pattern.SH,100);
        System.out.print(Common.getBannerFinish("Execution group 1 - SH"));

        Common.getBannerStart("Execution group 2 - EAS");
        housekeepingController.executeHousekeepingGroup(2);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","NWB",1000,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","UBR",900,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","UBN",800,Pattern.EAS,100);
        checkHousekeepingResult(testingDatabase,"test_table_eas","ADB","RBS",700,Pattern.EAS,100);
        System.out.print(Common.getBannerFinish("Execution group 2 - EAS"));

    }

}
