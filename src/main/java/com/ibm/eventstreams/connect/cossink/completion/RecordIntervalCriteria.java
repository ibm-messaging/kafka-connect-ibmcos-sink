package com.ibm.eventstreams.connect.cossink.completion;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * A Kafka record timestamp interval-based object completion criteria.
 *
 * Instances of this criteria complete the object storage object based
 * on a time interval from the timestamp in the first record.
 */
public class RecordIntervalCriteria implements ObjectCompletionCriteria {

    private final int intervalSec;

    private long startTimestamp;

    public RecordIntervalCriteria(final int intervalSec) {
        this.intervalSec = intervalSec;
    }

    @Override
    public FirstResult first(SinkRecord sinkRecord, AsyncCompleter asyncCompleter) {
        startTimestamp = sinkRecord.timestamp();
        return FirstResult.INCOMPLETE;
    }

    @Override
    public NextResult next(SinkRecord sinkRecord) {
        final long currentInterval = sinkRecord.timestamp() - startTimestamp;
        final boolean intervalExceeded =
                TimeUnit.SECONDS.convert(currentInterval, TimeUnit.MILLISECONDS) >= intervalSec;
        return intervalExceeded ? NextResult.COMPLETE_NON_INCLUSIVE : NextResult.INCOMPLETE;
    }

    @Override
    public void complete() {
    }
}
