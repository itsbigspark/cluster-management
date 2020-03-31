package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import com.bigspark.cloudera.management.common.utils.Utils;
import com.bigspark.cloudera.management.jobs.housekeeping.HousekeepingController;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class    OffloadRunner {
    public static void main(String[] args) throws Exception {
        Properties argsp = PropertyUtils.getProgramArgsAsProps(args);

        OffloadJob offloadJob = new OffloadJob(argsp);
        //offloadJob.executeTest();
    }
}
