package com.bigspark.cloudera.management.helpers;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileSystemHelperTest {

    DistributedFileSystem fileSystem;
    Configuration conf;
//    MiniDFSCluster.Builder builder;
//    MiniDFSCluster hdfsCluster;
    String hdfsURI;
    String testFileContent;
    FileSystemHelper fileSystemHelper;

    @BeforeEach
    void setUp() throws IOException {
        conf = new Configuration();
//        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "/tmp");
//        builder = new MiniDFSCluster.Builder(conf);
//        hdfsCluster = builder.build();
//        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
//        fileSystem = hdfsCluster.getFileSystem();
        //Create a path
        Path hdfswritepath = new Path("/testFile");
        testFileContent = "CLUSTER MANAGEMENT TEST";
        FSDataOutputStream outputStream=fileSystem.create(hdfswritepath);
        outputStream.writeBytes(testFileContent);
        outputStream.close();
        fileSystemHelper = new FileSystemHelper();
        fileSystemHelper.fs = this.fileSystem;
    }

    @Test
    void getConnection() throws IOException {
        assert(fileSystemHelper.getConnection().getUri().toString().equals(this.hdfsURI));
    }

    @Test
    void getFileContent() throws IOException, InterruptedException {
        assertEquals(fileSystemHelper.getFileContent("/testFile"),testFileContent);

    }

    @Test
    void writeFileContent() throws IOException, InterruptedException {
        try {
            fileSystemHelper.writeFileContent("/","testFile",testFileContent,false);
            assertEquals(fileSystemHelper.getFileContent("/testFile"),testFileContent);
            // Expect file to be in place as output from previous test
        } catch (Exception e){
            fileSystemHelper.writeFileContent("/","testFile",testFileContent,true);
            assertEquals(fileSystemHelper.getFileContent("/testFile"),testFileContent);
        }
    }

    @Test
    void copyToLocal() {
    }

    @Test
    void copyFromLocal() {
    }

    @Test
    void getUserHomeArea() {
    }

    @Test
    void delete() {
    }

    @Test
    void move() {
    }

    @Test
    void list() {
    }



}
