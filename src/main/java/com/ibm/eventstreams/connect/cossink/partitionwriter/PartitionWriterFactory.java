package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;

public interface PartitionWriterFactory {

    /**
     * Creates a new {@code PartitionWriter} instance
     * @param bucket the COS bucket that the writer will write into.
     * @param a set of criteria used to determine when sufficient Kafka records
     *             have been read and can be written as an object storage object.
     * @return
     */
    PartitionWriter newPartitionWriter(
            final Bucket bucket, final CompletionCriteriaSet completionCriteira);
}
