package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.enums.Pattern;

import java.io.*;
import java.util.Properties;

public class HousekeepingJobTestRunner {
    public static void main(String[] args) throws Exception {
        InputStream input=HousekeepingJobTestRunner.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        HousekeepingJobTest housekeepingJobTest = new HousekeepingJobTest();
        housekeepingJobTest.jobProperties=prop;
        housekeepingJobTest.execute();
    }

}
