package com.bigspark.cloudera.management.services;

import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.spark.sql.SparkSession;

import javax.jdo.metadata.Metadata;
import javax.naming.ConfigurationException;
import java.io.IOException;

public class ClusterManagementJob {

    private static ClusterManagementJob INSTANCE;
    protected SparkSession spark;
    protected FileSystem fs;
    protected Configuration hadoopConfiguration;
    protected HiveMetaStoreClient hiveMetaStoreClient;
    protected MetadataHelper metadataHelper;


    public ClusterManagementJob() throws IOException, MetaException, ConfigurationException {
        this.spark=SparkSession.builder()
                .appName("EDH CLUSTER MANAGEMENT - "+this.getClass().getName())
                .enableHiveSupport()
                .getOrCreate();
        this.hadoopConfiguration =(spark.sparkContext().hadoopConfiguration());
        this.fs=FileSystem.get(hadoopConfiguration);
        this.hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
        this.metadataHelper = new MetadataHelper();
    }

    public static ClusterManagementJob getInstance() throws IOException, MetaException, ConfigurationException {
        if(INSTANCE == null) INSTANCE = new ClusterManagementJob();
        return INSTANCE;
    }

}
