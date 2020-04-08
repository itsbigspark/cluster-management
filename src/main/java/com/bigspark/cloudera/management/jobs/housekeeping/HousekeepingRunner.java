package com.bigspark.cloudera.management.jobs.housekeeping;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HousekeepingRunner {

  static Logger logger = LoggerFactory.getLogger(HousekeepingRunner.class);

  public static void main(String[] args) throws Exception {
    logger.info("HousekeepingRunner Initialised");
    HousekeepingController housekeepingController = new HousekeepingController();
    Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
    int houseKeepingGroup = -1;
    if (argsp.containsKey("housekeepingGroup")) {
      houseKeepingGroup = PropertyUtils.getOptionalProperty(argsp, "housekeepingGroup", -1);
    }
      if (argsp.containsKey("housekeepingGroup")) {
         // housekeepingController.set PropertyUtils.getOptionalProperty(argsp, "isDryRun",  false);
          //housekeepingControlle

      }

    housekeepingController.execute(houseKeepingGroup);
    logger.info("HousekeepingRunner Completed");
  }

}
