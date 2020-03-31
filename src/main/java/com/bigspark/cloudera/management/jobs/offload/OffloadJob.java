package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableMetadata;
import com.bigspark.cloudera.management.helpers.AuditHelper;
import com.bigspark.cloudera.management.helpers.MetadataHelper;
import com.bigspark.cloudera.management.helpers.SparkHelper;
import com.bigspark.cloudera.management.jobs.ClusterManagementJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.Properties;

public class OffloadJob {

    public Properties jobProperties;
    public SparkHelper.AuditedSparkSession spark;
    public FileSystem fileSystem;
    public Configuration hadoopConfiguration;
    public HiveMetaStoreClient hiveMetaStoreClient;
    public MetadataHelper metadataHelper;
    public Boolean isDryRun;
    public ClusterManagementJob clusterManagementJob;
    public AuditHelper auditHelper;
    public TableMetadata tableMetadata;
    public SourceDescriptor sourceDescriptor;

    Pattern pattern;
        private Logger logger = LoggerFactory.getLogger(getClass());

    public OffloadJob(Properties properties) throws Exception {
        this.clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob);
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;

        if(properties.contains("fs.s3a.access.key")) {
            this.hadoopConfiguration.set("fs.s3a.access.key", properties.getProperty("fs.s3a.access.key"));
        }

        if(properties.contains("fs.s3a.secret.key")) {
            this.hadoopConfiguration.set("fs.s3a.secret.key", properties.getProperty("fs.s3a.secret.key"));
        }


        if(properties.contains("src") && properties.contains("tgt")) {
            Path src = new Path(properties.getProperty("src"));
            Path tgt = new Path(properties.getProperty("tgt"));
            int retVal = this.distCP(src, tgt);
        } else {
            logger.error("No src or tgt agrs passed");
        }


    }

    public OffloadJob() throws MetaException, SourceException, ConfigurationException, IOException {
        this.clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob);
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark,auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    }

    int distCP(Path src, Path tgt) throws Exception {

        DistCpOptions options = new DistCpOptions(src,tgt);
        options.setOverwrite(true);
        options.setBlocking(true);

        DistCp distCp = new DistCp(hadoopConfiguration, options);
        try {
            return distCp.run(new String[]{src.toString(), tgt.toString()});
        }
        catch(Exception e){
                logger.error("distCp : Exception occurred ", e);
            }
        return 0;
    }


    /**
     * Main entry point method for executing the offload process
     * @throws MetaException
     * @throws SourceException
     */
    void execute(TableMetadata tableMetadata) throws SourceException{
        this.tableMetadata = tableMetadata;
        this.sourceDescriptor = new SourceDescriptor(metadataHelper.getDatabase(tableMetadata.database),tableMetadata.tableDescriptor);
        this.pattern = MetadataHelper.getTableType(tableMetadata);
        if (this.pattern != null){

        }
    }

    void executeTest() throws Exception {
        distCP(new Path("/user/chris/jars/*"),new Path("s3a://s3tab/jars"));
    }
}
