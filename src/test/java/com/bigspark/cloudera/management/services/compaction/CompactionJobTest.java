package com.bigspark.cloudera.management.services.compaction;

import com.bigspark.cloudera.management.common.exceptions.SourceException;
import com.bigspark.cloudera.management.helpers.MetadataHelperTest;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import javax.naming.ConfigurationException;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class CompactionJobTest {

    CompactionJob compactionJob;

    @Before
    void setup() throws TException, ConfigurationException, IOException, SourceException {
        compactionJob = new CompactionJob();
        MetadataHelperTest metadataHelperTest = new MetadataHelperTest();
        metadataHelperTest.setUp();
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
        assert compactionJob.getRepartitionFactor((long) 217894092) == 2;
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
