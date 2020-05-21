package com.bigspark.cloudera.management.jobs.compaction;


import static mist.api.jdsl.Jdsl.stringArg;
import static mist.api.jdsl.Jdsl.withArgs;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import java.io.IOException;
import javax.naming.ConfigurationException;
import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionMistRunner extends MistFn {

  Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public Handle handle() {

    CompactionJob compactionJob = null;
   // try {
      //compactionJob = new CompactionJob();
   // } catch (IOException | ConfigurationException | SourceException | MetaException e) {
  //    e.printStackTrace();
   // }
    CompactionJob finalCompactionJob = compactionJob;
    return withArgs(stringArg("database"), stringArg("table")).
        withMistExtras().
        onSparkSessionWithHive((d, t, extra, spark) -> {
          logger.info(
              "Compaction job started with worker ID : " + extra.workerId() + ", job id : " + extra
                  .jobId() + ", args : ");
          logger.info("database : " + d);
          logger.info("table : " + t);
          //finalCompactionJob.executeMist(d, t, spark);
          return "complete";
        }).toHandle(JEncoders.stringEncoder());
  }
}

