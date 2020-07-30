package dev.bigspark.cloudera.management.jobs.compaction;

import java.io.InputStream;
import java.util.Properties;

public class CompactionJobTestRunner {

  public static void main(String[] args) throws Exception {
    InputStream input = CompactionJobTestRunner.class.getClassLoader()
        .getResourceAsStream("config.properties");
    Properties prop = new Properties();
    prop.load(input);
    CompactionJobIntegrationTests compactionJobIntegrationTests = new CompactionJobIntegrationTests();
    compactionJobIntegrationTests.jobProperties = prop;
    compactionJobIntegrationTests.execute();
  }
}
