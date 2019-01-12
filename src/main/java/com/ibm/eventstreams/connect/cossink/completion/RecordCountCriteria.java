package com.ibm.eventstreams.connect.cossink.completion;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A record count-based object completion criteria.
 *
 * Instances of this criteria complete the object storage object after a
 * certain number of {@code SinkRecord}'s have been received from Kafka.
 */
public class RecordCountCriteria implements ObjectCompletionCriteria {

    private final int recordsPerObject;
    private int count;

    public RecordCountCriteria(final int recordsPerObject) {
        this.recordsPerObject = recordsPerObject;
    }

    @Override
    public FirstResult first(SinkRecord sinkRecord, AsyncCompleter asyncCompleter) {
        count = 1;
        return count >= recordsPerObject ? FirstResult.COMPLETE : FirstResult.INCOMPLETE;
    }

    @Override
    public NextResult next(SinkRecord sinkRecord) {
        count++;
        return count >= recordsPerObject ? NextResult.COMPLETE_INCLUSIVE : NextResult.INCOMPLETE;
    }

    @Override
    public void complete() {
    }

}
