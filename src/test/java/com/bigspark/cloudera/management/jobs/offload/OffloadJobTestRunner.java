package com.bigspark.cloudera.management.jobs.offload;

import java.io.InputStream;
import java.util.Properties;

public class OffloadJobTestRunner {
    public static void main(String[] args) throws Exception {
        InputStream input= OffloadJobTestRunner.class.getClassLoader().getResourceAsStream("config.properties");
        Properties prop = new Properties();
        prop.load(input);
        OffloadJobIntegrationTests offloadJobTest = new OffloadJobIntegrationTests();
        offloadJobTest.jobProperties=prop;
        offloadJobTest.execute();
    }

}
