package com.bigspark.cloudera.management.common.model;

import org.apache.hadoop.hive.metastore.api.Database;

import java.util.ArrayList;

/**
 * Created by chris on 16/12/2019.
 */
public class SourceDescriptor {

    private Database database;
    private TableDescriptor table;
    private ArrayList<TableDescriptor> tables;

    public SourceDescriptor(Database database, ArrayList<TableDescriptor> tables) {
        this.database = database;
        this.tables = tables;
    }

    public SourceDescriptor(Database database, TableDescriptor table) {
        this.database = database;
        this.table = table;
    }

    @Override
    public String toString() {
        return "SourceDescriptor{" +
                "database=" + database +
                ", table=" + table +
                '}';
    }
}


