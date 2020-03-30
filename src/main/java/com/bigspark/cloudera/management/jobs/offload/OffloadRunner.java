package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingController;
import org.apache.hadoop.conf.Configuration;

public class    OffloadRunner {
    public static void main(String[] args) throws Exception {
        OffloadJob offloadJob = new OffloadJob();
        offloadJob.executeTest();
    }
}
