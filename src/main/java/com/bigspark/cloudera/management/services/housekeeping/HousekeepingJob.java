package com.bigspark.cloudera.management.services.housekeeping;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.bigspark.cloudera.management.services.ClusterManagementJob;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class HousekeepingJob extends ClusterManagementJob {

    Logger log = LoggerFactory.getLogger(getClass());

    HousekeepingJob() throws IOException, MetaException, ConfigurationException {
        super();
    }

    void execute() throws MetaException, SourceException {
        List<String> allDatabases;
        allDatabases = metadataHelper.getAllDatabases();
        for (String database: allDatabases){
            ArrayList<Table> allTablesFromDatabase = metadataHelper.getAllTablesFromDatabase(database);
            ArrayList<TableDescriptor> allTableDescriptors = metadataHelper.getAllTableDescriptors(allTablesFromDatabase);
            for (TableDescriptor table : allTableDescriptors){
                if (table.isPartitioned()){
                    List<Partition> partitionList = table.getPartitionList();
                    partitionList.toString();
                }
            }
        }
    }

}
