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
