package com.bigspark.cloudera.management.services.compaction;


import static mist.api.jdsl.Jdsl.*;
import mist.api.Handle;
import mist.api.MistFn;
import mist.api.jdsl.JEncoders;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;

public class CompactionMistRunner extends MistFn {

        Logger logger = LoggerFactory.getLogger(getClass());

        @Override
        public Handle handle() {

            CompactionJob compactionJob = null;
            try {
                compactionJob = new CompactionJob();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (MetaException e) {
                e.printStackTrace();
            } catch (ConfigurationException e) {
                e.printStackTrace();
            }
            CompactionJob finalCompactionJob = compactionJob;
            return withArgs(stringArg("database"), stringArg("table")).
                    withMistExtras().
                    onSparkSessionWithHive((d, t,extra,spark) -> {
                        logger.info("Compaction job started with worker ID : "+extra.workerId()+ ", job id : "+extra.jobId()+", args : "  );
                        logger.info("database : "+d  );
                        logger.info("table : "+t  );
                        finalCompactionJob.executeMist(d,t, spark);
                        return "complete";
                    }).toHandle(JEncoders.stringEncoder());
        }
}

