package com.bigspark.cloudera.management.services.compaction;

public class CompactionRunner {
    public static void main(String[] args) throws Exception {
        CompactionJob compactionJob = new CompactionJob();
        compactionJob.execute();
    }
}
