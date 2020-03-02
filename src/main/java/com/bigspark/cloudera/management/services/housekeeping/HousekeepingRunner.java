package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.services.compaction.CompactionJob;

public class HousekeepingRunner {
    public static void main(String[] args) throws Exception {
        HousekeepingJob housekeepingJob = new HousekeepingJob();
        housekeepingJob.execute();
    }
}
