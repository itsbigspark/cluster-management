package dev.bigspark.cloudera.management.jobs.offload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
//52733
//60521
//57906
public class Offload {
    private static Logger logger = LoggerFactory.getLogger(Offload.class);

    public static void main(String[] args) throws Exception {
        try(SparkSession spark = SparkSession.builder().getOrCreate()) {
            Configuration conf = new Configuration();
            //FileSystem fs = FileSystem.get(conf);
            String key = args[0];
            String secret = args[1];
            String bucket = "bigspark-software";
            conf.set("fs.s3a.access.key", key);
            conf.set("fs.s3a.secret.key", secret);
            FileSystem fs = FileSystem.get(conf);
            Path src = new Path("/user/bigspark");
            Path tgt = new Path("s3a://bigspark-software/distcp-test/");
            DistCpOptions options = new DistCpOptions(src, tgt);
            options.setOverwrite(true);
            // options.setSyncFolder(true);
            options.setBlocking(true);
            DistCp distCp = new DistCp(conf, options);
            distCp.run(new String[]{src.toString(), tgt.toString()});
        }
    }
}
