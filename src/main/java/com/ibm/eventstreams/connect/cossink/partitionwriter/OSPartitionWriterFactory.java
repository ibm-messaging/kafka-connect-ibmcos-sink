package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;

public class OSPartitionWriterFactory implements PartitionWriterFactory {

    public OSPartitionWriterFactory() {
    }

    @Override
    public PartitionWriter newPartitionWriter(
            final Bucket bucket, final CompletionCriteriaSet completionCriteria) {
        return new OSPartitionWriter(bucket, completionCriteria);
    }

}
