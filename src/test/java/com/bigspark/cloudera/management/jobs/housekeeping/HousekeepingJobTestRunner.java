package com.bigspark.cloudera.management.jobs.housekeeping;

import java.io.*;
import java.util.Properties;

public class HousekeepingJobTestRunner {
    public static void main(String[] args) throws Exception {
        InputStream input=HousekeepingJobTestRunner.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        HousekeepingJobIntegrationTest housekeepingJobTest = new HousekeepingJobIntegrationTest();
        housekeepingJobTest.jobProperties=prop;
        housekeepingJobTest.execute();
    }

}
