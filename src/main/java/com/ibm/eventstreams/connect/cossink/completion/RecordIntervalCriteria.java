/*
 * Copyright 2019 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
