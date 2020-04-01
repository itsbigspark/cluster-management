package com.bigspark.cloudera.management.jobs.compaction;

import com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingJobIntegrationTest;
import com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingJobTestRunner;

import java.io.InputStream;
import java.util.Properties;

public class CompactionJobTestRunner {

    public static void main(String[] args) throws Exception {
        InputStream input= CompactionJobTestRunner.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        CompactionJobIntegrationTest compactionJobIntegrationTest = new CompactionJobIntegrationTest();
        compactionJobIntegrationTest.jobProperties=prop;
        compactionJobIntegrationTest.execute();
    }
}
