package com.bigspark.cloudera.management.jobs.offload;
import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffloadRunner {

  static Logger logger = LoggerFactory.getLogger(OffloadRunner.class);

  public static void main(String[] args) throws Exception {
    logger.info("OffloadRunner Initialised");
    OffloadController OffloadController = new OffloadController();
    Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
    int offloadGroup = -1;
    if (argsp.containsKey("Group")) {
      offloadGroup = PropertyUtils.getOptionalProperty(argsp, "Group", -1);
    }
    if (argsp.containsKey("Group")) {

    }
    OffloadController.execute(offloadGroup);
    logger.info("OffloadRunner Completed");
  }
}
