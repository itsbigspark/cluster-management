package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;

import java.io.InputStream;
import java.util.Properties;

public class HousekeepingJobTestRunner {
    public static void main(String[] args) throws Exception {
        System.out.println("Test startup");
        InputStream input=HousekeepingJobTestRunner.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        HousekeepingJobTest housekeepingJobTest = new HousekeepingJobTest();
        housekeepingJobTest.jobProperties=prop;
        housekeepingJobTest.execute();
    }
}
