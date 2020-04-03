package com.bigspark.cloudera.management.jobs.housekeeping;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HousekeepingJobTest {

    private final HousekeepingJob housekeepingJob;

    public HousekeepingJobTest(HousekeepingJob housekeepingJob) {
        this.housekeepingJob = housekeepingJob;
    }

    @Test
    void getTablePartitionMonthEnds() {
    }

    @Test
    void calculatePurgeCeiling() {
        assertEquals(
                LocalDate.of(2020,01,01)
                ,housekeepingJob.calculatePurgeCeiling(10, LocalDate.of(2020,01,11))
        );
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
