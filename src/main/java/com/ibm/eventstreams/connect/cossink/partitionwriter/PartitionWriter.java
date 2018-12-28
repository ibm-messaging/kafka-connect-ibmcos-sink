package com.ibm.eventstreams.connect.cossink.partitionwriter;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * PartitionWriter instances writes {@code SinkRecords} for one Kafka partition
 * to IBM COS. An instance is created at the point a partition is assigned to
 * a SinkTask and is closed when the partition is no longer assigned to the task.
 */
public interface PartitionWriter {

    /**
     * preCommit is called just before the connector commits offsets for this partition.
     * @return the offset to commit for this partition, or <code>null</code> if the offset
     *         hasn't changed since the last time {@code #preCommit()} was called.
     */
    Long preCommit();

    /**
     * put passes a {@code SinkRecord} to this writer for processing.
     * @param record
     */
    void put(SinkRecord record);

    /**
     * close is called when the writer is no longer required.
     */
    void close();
}
