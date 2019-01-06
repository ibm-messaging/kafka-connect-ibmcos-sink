package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;

public interface PartitionWriterFactory {

    /**
     * Creates a new {@code PartitionWriter} instance
     * @param deadlineSec the number of seconds before all of the Kafka records
     *             received so far are used to make up an object storage object.
     * @param intervalSec the largest permitted interval between the first and
     *             last message timestamps that make up an object storage object.
     * @param recordsPerObject the number of Kafka records used to make up each
     *             object storage object.
     * @param bucket the COS bucket that the writer will write into.
     * @return
     */
    PartitionWriter newPartitionWriter(int deadlineSec, final int intervalSec, int recordsPerObject, Bucket bucket);
}
