package com.bigspark.cloudera.management.services.compaction;

public class CompactionJobTestRunner {

    public static void main(String[] args) throws Exception {
        CompactionJobIntegrationTest compactionJobIntegrationTest = new CompactionJobIntegrationTest();
        compactionJobIntegrationTest.execute();
    }
}
