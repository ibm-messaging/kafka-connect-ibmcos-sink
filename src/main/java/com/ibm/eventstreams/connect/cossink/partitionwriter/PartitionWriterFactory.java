package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;

public interface PartitionWriterFactory {

    /**
     * Creates a new {@code PartitionWriter} instance
     * @param bucket the COS bucket that the writer will write into.
     * @return
     */
    PartitionWriter newPartitionWriter(Bucket bucket);
}
