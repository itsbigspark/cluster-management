package com.bigspark.cloudera.management.jobs.purging;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurgingRunner {

  static Logger logger = LoggerFactory.getLogger(PurgingRunner.class);

  public static void main(String[] args) throws Exception {
    logger.info("PurgingRunner Initialised");
    PurgingController PurgingController = new PurgingController();
    Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
    int purgingGroup = -1;
    if (argsp.containsKey("purgingGroup")) {
      purgingGroup = PropertyUtils.getOptionalProperty(argsp, "purgingGroup", -1);
    }
    if (argsp.containsKey("purgingGroup")) {

    }
    PurgingController.execute(purgingGroup);
    logger.info("PurgingRunner Completed");
  }

}
