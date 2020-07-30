package dev.bigspark.cloudera.management.jobs.purging;

import java.io.InputStream;
import java.util.Properties;

public class PurgingJobTestRunner {

  public static void main(String[] args) throws Exception {
    InputStream input = PurgingJobTestRunner.class.getClassLoader()
        .getResourceAsStream("config.properties");
    Properties prop = new Properties();
    prop.load(input);
    PurgingJobIntegrationTests PurgingJobTest = new PurgingJobIntegrationTests();
    PurgingJobTest.jobProperties = prop;
    PurgingJobTest.execute();
  }

}
