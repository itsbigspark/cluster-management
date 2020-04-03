package com.bigspark.cloudera.management.jobs.housekeeping;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class HousekeepingRunner {
    static Logger logger = LoggerFactory.getLogger(HousekeepingRunner.class);
    public static void main(String[] args) throws Exception {
        logger.debug("HousekeepingRunner Initialised");
        HousekeepingController housekeepingController = new HousekeepingController();
        Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
        int houseKeepingGroup = -1;
        if(argsp.containsKey("housekeepingGroup")) {
            houseKeepingGroup = PropertyUtils.getOptionalProperty(argsp, "housekeepingGroup", -1);
        }
        housekeepingController.execute(houseKeepingGroup);
        logger.debug("HousekeepingRunner Completed");
    }

}
