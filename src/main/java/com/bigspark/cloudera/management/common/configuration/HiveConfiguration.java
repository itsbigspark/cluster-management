package com.bigspark.cloudera.management.common.configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;

import javax.naming.ConfigurationException;

public class HiveConfiguration {

    public HiveConf hiveConf() throws ConfigurationException {
        HiveConf hiveConf = new HiveConf();
        String hiveSitePath;

        if(System.getenv("HIVE_CONF_DIR") != null) {
            hiveSitePath = System.getenv("HIVE_CONF_DIR") + "/hive-site.xml";
        }
        else if(System.getenv("HADOOP_CONF_DIR") != null) {
            hiveSitePath = System.getenv("HADOOP_CONF_DIR") + "/hive/hive-site.xml";
        } else {
            throw new ConfigurationException("HADOOP_CONF_DIR or HIVE_CONF_DIR not provided");
        }
        hiveConf.addResource(new Path(hiveSitePath));
        return hiveConf;
    }

    public HiveMetaStoreClient hiveMetaStoreClient() throws ConfigurationException, MetaException {
        HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf());
        return hiveMetaStoreClient;
    }
}
