package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableMetadata;
import com.bigspark.cloudera.management.common.utils.PropertyUtils;
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

        logger.info(PropertyUtils.dumpProperties(properties));

        if(properties.containsKey("fs.s3a.access.key")) {
            logger.info("Setting Configuration 'fs.s3a.access.key' with passed program argument");
            this.hadoopConfiguration.set("fs.s3a.access.key", properties.getProperty("fs.s3a.access.key"));
        }

        if(properties.containsKey("fs.s3a.secret.key")) {
            logger.warn("Setting Configuration 'fs.s3a.secret.key' with passed program argument.  This method is not recommended");
            this.hadoopConfiguration.set("fs.s3a.secret.key", properties.getProperty("fs.s3a.secret.key"));
        }


        if(properties.containsKey("src") && properties.containsKey("tgt")) {
            logger.info("Setting src and tgt from passed program arguments");
            Path src = new Path(properties.getProperty("src"));
            Path tgt = new Path(properties.getProperty("tgt"));
            int retVal = this.distCP(src, tgt);
        } else {
            logger.error("No src or tgt agrs passed");
        }

        logger.info("Finished distpCp");


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

        logger.info(String.format("Copying '%s' to '%s'", src.toString(), tgt.toString()));
        DistCpOptions options = new DistCpOptions(src,tgt);
        options.setOverwrite(true);
        options.setBlocking(true);

        DistCp distCp = new DistCp(hadoopConfiguration, options);
        try {
            logger.info("Starting distCp");
            return distCp.run(new String[]{src.toString(), tgt.toString()});
        }
        catch(Exception e){
                logger.error("distCp : Exception occurred ", e);
                throw e;
            }
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
