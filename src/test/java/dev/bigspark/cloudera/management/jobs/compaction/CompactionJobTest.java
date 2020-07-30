package dev.bigspark.cloudera.management.jobs.compaction;

import static javolution.testing.TestContext.assertEquals;

import java.io.IOException;
import javax.naming.ConfigurationException;

import dev.bigspark.hadoop.exceptions.SourceException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompactionJobTest {

  CompactionJob compactionJob;

  public CompactionJobTest(CompactionJob compactionJob)
      throws MetaException, SourceException, ConfigurationException, IOException {
    this.compactionJob = compactionJob;
  }

  @BeforeEach
  void setup() throws TException, ConfigurationException, IOException, SourceException {
//        compactionJob = new CompactionJob();
//        MetadataHelperTest metadataHelperTest = new MetadataHelperTest();
//        metadataHelperTest.setUp();
  }

  @Test
  void getBlocksize() {
  }

  @Test
  void getDatabases() {
  }

  @Test
  void getTables() {
  }

  @Test
  void getTablePartitions() {
  }

  @Test
  void getTableLocation() {
  }

  @Test
  void getFileCountTotalSizePair() {
  }

  @Test
  void getFileCountTotalSize() {
  }

  @Test
  void isCompactionCandidate() {
    assert compactionJob.isCompactionCandidate((long) 3, (long) 417894092);
  }

  @Test
  void getRepartitionFactor() {
    //Assumes standard block size of 128MB
    assertEquals(
        java.util.Optional.ofNullable(compactionJob.getRepartitionFactor((long) 217894092)), 2);
  }

  @Test
  void processTable() {
  }

  @Test
  void processPartition() {
  }

  @Test
  void compactLocation() {
  }

  @Test
  void reconcileOutput() {
  }

  @Test
  void setTrashBaseLocation() {
  }

  @Test
  void trashOriginalData() {
  }

  @Test
  void resolvePartition() {
  }
}
