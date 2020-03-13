package com.bigspark.cloudera.management.services.compaction;

import com.bigspark.cloudera.management.services.housekeeping.HousekeepingJobTestRunner;
import java.io.InputStream;
import java.util.Properties;

public class CompactionJobTestRunner {

    public static void main(String[] args) throws Exception {
        CompactionJobTest compactionJobTest = new CompactionJobTest();
        compactionJobTest.execute();
    }
}
