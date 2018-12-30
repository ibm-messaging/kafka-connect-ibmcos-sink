package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;

public interface PartitionWriterFactory {

    /**
     * Creates a new {@code PartitionWriter} instance
     * @param recordsPerObject the number of Kafka records used to make up each
     *             object storage object.
     * @param bucket the COS bucket that the writer will write into.
     * @return
     */
    PartitionWriter newPartitionWriter(int recordsPerObject, Bucket bucket);
}
