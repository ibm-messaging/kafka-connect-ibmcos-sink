package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;

public class OSPartitionWriterFactory implements PartitionWriterFactory {

    public OSPartitionWriterFactory() {
    }

    @Override
    public PartitionWriter newPartitionWriter(final Bucket bucket) {
        return new OSPartitionWriter(bucket);
    }

}
