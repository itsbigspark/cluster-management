package com.bigspark.cloudera.management.helpers;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.common.model.TableDescriptor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class MetadataHelperTest {

    Table tableBase;
    Table tableSourceHistory;
    Table tableEAS;
    HiveMetaStoreClient hiveMetaStoreClient;
    MetadataHelper metadataHelper;


    @Before
    public void setUp() throws IOException, TException, ConfigurationException {
        this.metadataHelper = new MetadataHelper();
        this.hiveMetaStoreClient = new HiveMetaStoreClient(new HiveConf());
        tableBase = new Table();
        tableBase.setDbName("testDB");
        tableBase.setTableName("testTable");
        tableBase.setSd(new StorageDescriptor());
        tableBase.getSd().setCols(Arrays.asList(new FieldSchema("id", "int", null), new FieldSchema("name", "string", null)));
        tableBase.getSd().setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
        tableBase.getSd().setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
        tableBase.getSd().setSerdeInfo(new SerDeInfo());
        tableBase.getSd().getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        hiveMetaStoreClient.createTable(tableBase);

        tableSourceHistory = tableBase;
        tableSourceHistory.setTableName("testTableSH");
        tableSourceHistory.setPartitionKeys(Arrays.asList(new FieldSchema("edi_business_day", "date", null)));
        hiveMetaStoreClient.createTable(tableSourceHistory);
        hiveMetaStoreClient.appendPartition("testDB","testTableSH",Arrays.asList("2020-01-20"));

        tableEAS = tableBase;
        tableEAS.setTableName("testTableEAS");
        tableEAS.setPartitionKeys(Arrays.asList(
                new FieldSchema("edi_business_day", "date", null),
                new FieldSchema("source_sys_id", "string", null),
                new FieldSchema("source_sys_inst_id", "string", null)
        ));
        hiveMetaStoreClient.createTable(tableEAS);
        hiveMetaStoreClient.appendPartition("testDB","testTableEAS",Arrays.asList("ADB","UBR","2020-01-20"));

        //        hiveMetaStoreClient.close();
    }

    @Test
    void getTableDescriptor() throws SourceException {
        TableDescriptor tdBase = this.metadataHelper.getTableDescriptor(this.tableBase);
        assert(tdBase.isPartitioned().equals(false));
        assert(tdBase.getDatabaseName().equals("testDB"));
        assert(tdBase.getTableName().equals("testTable"));
        TableDescriptor tdSH = this.metadataHelper.getTableDescriptor(this.tableSourceHistory);
        assert(tdSH.isPartitioned().equals(true));
        assert(tdSH.getPartitionList().size() == 1);
        assert(tdSH.getTableName().equals("testTableSH"));
//        assert(tdSH.getPartitionList().get(0).equals("edi_business_day=2020-01-20"));
        TableDescriptor tdEAS = this.metadataHelper.getTableDescriptor(this.tableEAS);
        assert(tdEAS.isPartitioned().equals(true));
        assert(tdSH.getPartitionList().size() == 1);
//        assert(tdSH.getPartitionList().get(0).equals("edi_business_day=2020-01-20","src_sys_id=ADB","src_sys_inst_id=UBR"));
        assert(tdEAS.getTableName().equals("testTableEAS"));
    }

    @Test
    void getAllTableDescriptors() throws SourceException {
        ArrayList<TableDescriptor> tableDescriptors =
                metadataHelper.getAllTableDescriptors(metadataHelper.getAllTablesFromDatabase("testDB"));
        assert tableDescriptors.size()==3;
        assert tableDescriptors.contains(this.metadataHelper.getTableDescriptor(this.tableBase));
        assert tableDescriptors.contains(this.metadataHelper.getTableDescriptor(this.tableSourceHistory));
        assert tableDescriptors.contains(this.metadataHelper.getTableDescriptor(this.tableEAS));
    }

    @Test
    void getTable() {
    }

    @Test
    void getAllTablesFromDatabase() throws SourceException {
        ArrayList<Table> tables = metadataHelper.getAllTablesFromDatabase("testDB");
        assert tables.contains(tableBase);
        assert tables.contains(tableSourceHistory);
        assert tables.contains(tableEAS);
    }

    @Test
    void getTablePartition() throws SourceException {
        assert metadataHelper.getTablePartition("testDB","testTableSH","edi_business_day=2020-01-20").get(0) != null;
    }

    @Test
    void getTablePartitions() {
    }

    @Test
    void getDatabase() {
    }

    @Test
    void getAllDatabases() {
    }

    @Test
    void getPartitionDate() {
    }

    @Test
    void stringToDate() {
    }
}
