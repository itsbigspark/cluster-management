package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.helpers.SparkHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
//52733
//60521
//57906
public class Offload {
    private static Logger logger = LoggerFactory.getLogger(Offload.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //FileSystem fs = FileSystem.get(conf);
        String key = "AKIARYZZY3SV5QOL6MNI";
        String secret = "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";
        String bucket = "bigspark-software";
        conf.set("fs.s3a.access.key",key);
        conf.set("fs.s3a.secret.key",secret);

        Path src = new Path("/user/cloudera/test.txt");
        Path tgt = new Path("s3a://bigspark-software/distcp-test/test.txt");
        DistCpOptions options = new DistCpOptions(src,tgt);
        options.setOverwrite(true);
        DistCp distCp = new DistCp(conf, options);
        try {
             ToolRunner.run(distCp,new String[]{src.toString(),tgt.toString()});
        }
        catch(Exception e){
            logger.error("distCp : Exception occurred ", e);
        }
    }
}
