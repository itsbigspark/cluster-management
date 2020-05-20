package com.bigspark.cloudera.management.jobs.compaction;

import com.bigspark.cloudera.management.common.utils.PropertyUtils;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionRunner {

  static Logger logger = LoggerFactory.getLogger(CompactionRunner.class);

  public static void main(String[] args) throws Exception {
    logger.info("CompactionRunner Initialised");
    CompactionController CompactionController = new CompactionController();
    Properties argsp = PropertyUtils.getProgramArgsAsProps(args);
    int compactionGroup = -1;
    if (argsp.containsKey("Group")) {
      compactionGroup = PropertyUtils.getOptionalProperty(argsp, "Group", -1);
    }
    if (argsp.containsKey("Group")) {

    }
    CompactionController.execute(compactionGroup);
    logger.info("CompactionRunner Completed");
  }
}
