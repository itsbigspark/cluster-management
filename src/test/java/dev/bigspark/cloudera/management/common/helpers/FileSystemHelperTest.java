package dev.bigspark.cloudera.management.common.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import dev.bigspark.hadoop.helpers.FileSystemHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

//import org.apache.hadoop.hdfs.MiniDFSCluster;

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
    FSDataOutputStream outputStream = fileSystem.create(hdfswritepath);
    outputStream.writeBytes(testFileContent);
    outputStream.close();
    fileSystemHelper = new FileSystemHelper();
//    FileSystemHelper. = this.fileSystem;
  }

  @Test
  void getConnection() throws IOException {
    assert (FileSystemHelper.getConnection().getUri().toString().equals(this.hdfsURI));
  }

  @Test
  void getFileContent() throws IOException, InterruptedException {
    assertEquals(FileSystemHelper.getFileContent("/testFile"), testFileContent);

  }

  @Test
  void writeFileContent() throws IOException, InterruptedException {
    try {
      FileSystemHelper.writeFileContent("/", "testFile", testFileContent, false);
      assertEquals(FileSystemHelper.getFileContent("/testFile"), testFileContent);
      // Expect file to be in place as output from previous test
    } catch (Exception e) {
      FileSystemHelper.writeFileContent("/", "testFile", testFileContent, true);
      assertEquals(FileSystemHelper.getFileContent("/testFile"), testFileContent);
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
