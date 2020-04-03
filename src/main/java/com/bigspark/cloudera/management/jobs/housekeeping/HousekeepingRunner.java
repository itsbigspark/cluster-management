package com.bigspark.cloudera.management.jobs.housekeeping;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;

import java.util.Properties;

public class HousekeepingRunner {
    public static void main(String[] args) throws Exception {
        HousekeepingController housekeepingController = new HousekeepingController();
        Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
        int houseKeepingGroup = -1;
        if(argsp.containsKey("housekeepingGroup")) {
            houseKeepingGroup = PropertyUtils.getOptionalProperty(argsp, "housekeepingGroup", -1);
        }
        housekeepingController.execute(houseKeepingGroup);
    }
}
