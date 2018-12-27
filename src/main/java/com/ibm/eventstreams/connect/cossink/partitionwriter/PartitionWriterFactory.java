package com.ibm.eventstreams.connect.cossink.partitionwriter;

import com.ibm.cos.Bucket;

public interface PartitionWriterFactory {
    PartitionWriter newPartitionWriter(Bucket bucket);
}
