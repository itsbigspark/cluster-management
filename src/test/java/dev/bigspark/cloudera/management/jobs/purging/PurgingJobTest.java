package dev.bigspark.cloudera.management.jobs.purging;

import java.util.List;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Test;

class PurgingJobTest {

  private final PurgingJob PurgingJob;

  public PurgingJobTest(PurgingJob PurgingJob) {
    this.PurgingJob = PurgingJob;
  }

  @Test
  void getTablePartitionMonthEnds() {
  }

  @Test
  void calculatePurgeCeiling() {
    // assertEquals(
    //        LocalDate.of(2020,01,01)
    //        ,PurgingJob.calculatePurgeCeiling(10, LocalDate.of(2020,01,11))
    //  );
  }

  @Test
  void dropHivePartition() {
  }

  @Test
  void purgeHivePartition() {
  }

  @Test
  void getAllPurgeCandidates() {
    List<Partition> partitionList;
    // Really difficult constructor!
  }

  @Test
  void createMonthEndSwingTable() {
  }

  @Test
  void setTrashBaseLocation() {
  }

  @Test
  void trashDataOutwithRetention() {
  }

  @Test
  void cleanUpPartitions() {
  }

  @Test
  void resolveSourceTableWithSwingTable() {
  }

  @Test
  void purgeHDFSPartition() {
  }

  @Test
  void invalidateMetadata() {
  }

  @Test
  void verifyPartitionKey() {
  }

  @Test
  void testVerifyPartitionKey() {
  }

  @Test
  void returnPartitionDate() {
  }

  @Test
  void getTableType() {
  }
}
