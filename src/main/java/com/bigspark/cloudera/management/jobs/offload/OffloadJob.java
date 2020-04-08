package com.bigspark.cloudera.management.jobs.offload;

import com.bigspark.cloudera.management.common.enums.Pattern;
import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.metadata.HousekeepingMetadata;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
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
    public HousekeepingMetadata housekeepingMetadata;
    public SourceDescriptor sourceDescriptor;

    Pattern pattern;
    private Logger logger = LoggerFactory.getLogger(getClass());

    public OffloadJob() throws MetaException, SourceException, ConfigurationException, IOException {
        this.clusterManagementJob = ClusterManagementJob.getInstance();
        this.auditHelper = new AuditHelper(clusterManagementJob, "Storage offload job");
        this.spark = new SparkHelper.AuditedSparkSession(clusterManagementJob.spark, auditHelper);
        this.fileSystem = clusterManagementJob.fileSystem;
        this.hadoopConfiguration = clusterManagementJob.hadoopConfiguration;
        this.metadataHelper = clusterManagementJob.metadataHelper;
        this.isDryRun = clusterManagementJob.isDryRun;
        this.jobProperties = clusterManagementJob.jobProperties;
        this.hiveMetaStoreClient = clusterManagementJob.hiveMetaStoreClient;
    }

    int distCP(Path src, Path tgt) throws Exception {

        DistCpOptions options = new DistCpOptions(src, tgt);
        options.setOverwrite(true);
        hadoopConfiguration.set("fs.s3a.endpoint", "object.ecstestdrive.com");
        hadoopConfiguration.set("fs.s3a.awsAccessKeyId", "131855586862166345@ecstestdrive.emc.com");
        hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", "Q+f/ypU/Ii6s2tWLmpxyaIVgxT4+rBLWroAO4ufS");
        DistCp distCp = new DistCp(hadoopConfiguration, options);
        return distCp.run(new String[]{src.toString(), tgt.toString()});
    }


    /**
     * Main entry point method for executing the offload process
     *
     * @throws MetaException
     * @throws SourceException
     */
    void execute(HousekeepingMetadata housekeepingMetadata) throws SourceException {
        this.housekeepingMetadata = housekeepingMetadata;
        this.sourceDescriptor = new SourceDescriptor(metadataHelper.getDatabase(housekeepingMetadata.database), housekeepingMetadata.tableDescriptor);
        this.pattern = MetadataHelper.getTableType(housekeepingMetadata);
        if (this.pattern != null) {

        }
    }

}
