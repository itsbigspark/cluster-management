package com.bigspark.cloudera.management.services.compaction;


import static mist.api.jdsl.Jdsl.*;
import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionMistRunner extends MistFn {

        Logger logger = LoggerFactory.getLogger(getClass());

        @Override
        public Handle handle() {

            CompactionJob compactionJob = new CompactionJob();
            return withArgs(stringArg("database"), stringArg("table")).
                    withMistExtras().
                    onSparkSessionWithHive((d, t,extra,spark) -> {
                        logger.info("Compaction job started with worker ID : "+extra.workerId()+ ", job id : "+extra.jobId()+", args : "  );
                        logger.info("database : "+d  );
                        logger.info("table : "+t  );
                        compactionJob.executeMist(d,t, spark);
                        return "complete";
                    }).toHandle(JEncoders.stringEncoder());
        }
}

