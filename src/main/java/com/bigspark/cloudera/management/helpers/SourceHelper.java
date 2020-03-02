package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.SourceDescriptor;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;


import javax.naming.ConfigurationException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.bigspark.cloudera.management.helpers.MetadataHelper.*;

/**
 * Created by chris on 11/12/2019.
 */

public class SourceHelper {

    List<String> hiveMetaStoreDatabases;
    MetadataHelper metadataHelper;


    private SourceDescriptor handleURI(URI uri) throws SourceException {

        String[] pathElements = uri.getPath().split("/");
        ArrayList<String> pathArray = new ArrayList<String>(Arrays.asList(pathElements));
        SourceDescriptor sourceDescriptor;

        if (uri.getScheme().equals("hdfs")) {
            //   hdfs:///user/finlari
        }

        String database = pathArray.get(1);
        if (uri.getScheme().equals("database")) {
            //     database://database
            ArrayList<Table> tableList = metadataHelper.getAllTablesFromDatabase(database);
            sourceDescriptor = new SourceDescriptor(
                    metadataHelper.getDatabase(database)
                    , metadataHelper.getAllTableDescriptors(tableList));
            return sourceDescriptor;
        }

        String table = pathArray.get(2);
        if (uri.getScheme().equals("table")) {
            //     table://database/table
            Table t = metadataHelper.getTable(database, table);
            sourceDescriptor = new SourceDescriptor(
                    metadataHelper.getDatabase(database)
                    , metadataHelper.getTableDescriptor(t));
            return sourceDescriptor;
        }

        List<String> partitionElements = pathArray.subList(3, pathArray.size() + 2);
        if (uri.getScheme().equals("partition")) {
            String pk = String.join("/", partitionElements);
            //     partition://database/table/p1=1/p2=2/p2=3
            Table t = metadataHelper.getTable(database, table);
            sourceDescriptor = new SourceDescriptor(
                    metadataHelper.getDatabase(database)
                    , metadataHelper.getTableDescriptor(t));
            return sourceDescriptor;
        } else {
            throw new SourceException("Unexpected URI scheme provided");
        }
    }

    private ArrayList<SourceDescriptor> getAllDatabaseSourceDescriptors() throws SourceException, MetaException, ConfigurationException {
        this.metadataHelper = new MetadataHelper();
        ArrayList<SourceDescriptor> allSourceDescriptors = Lists.newArrayList();
        for (String database:hiveMetaStoreDatabases){
            Database database_  = metadataHelper.getDatabase(database);
            ArrayList<Table> tables = metadataHelper.getAllTablesFromDatabase(database);
            ArrayList<TableDescriptor> tableDescriptors = metadataHelper.getAllTableDescriptors(tables);
            allSourceDescriptors.add(new SourceDescriptor(database_,tableDescriptors));
        }
        return allSourceDescriptors;
    }
}
