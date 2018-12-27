package com.ibm.eventstreams.connect.cossink.partitionwriter;

import org.apache.kafka.connect.sink.SinkRecord;

public interface PartitionWriter {

    // TODO: how best to avoid re-committing the same offset if no progress has been
    //       made between calls?
    Long preCommit();

    void put(SinkRecord record);

    void close();
}
