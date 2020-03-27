package com.bigspark.cloudera.management.jobs.compaction;

public class CompactionJobTestRunner {

    public static void main(String[] args) throws Exception {
        CompactionJobIntegrationTest compactionJobIntegrationTest = new CompactionJobIntegrationTest();
        compactionJobIntegrationTest.execute();
    }
}
