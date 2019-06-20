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

import com.ibm.eventstreams.connect.cossink.deadline.DeadlineCanceller;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineListener;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;

/**
 * A wall-clock time-based object completion criteria.
 *
 * Instances of this criteria complete the object storage object based on a
 * deadline which starts counting down from the point in time when the first
 * record in a new object is received.
 */
public class DeadlineCriteria implements ObjectCompletionCriteria, DeadlineListener {

    private final DeadlineService deadlineService;
    private final int deadlineSec;

    private DeadlineCanceller canceller;

    public DeadlineCriteria(final DeadlineService deadlineService, final int deadlineSec) {
        this.deadlineService = deadlineService;
        this.deadlineSec = deadlineSec;
    }

    @Override
    public FirstResult first(SinkRecord sinkRecord, AsyncCompleter asyncCompleter) {
        canceller = deadlineService.schedule(this, deadlineSec, TimeUnit.SECONDS, asyncCompleter);
        return FirstResult.INCOMPLETE;
    }

    @Override
    public NextResult next(SinkRecord sinkRecord) {
        return NextResult.INCOMPLETE;
    }

    @Override
    public void complete() {
        // There's a slight inefficiency here as this implementation will (needlessly)
        // try to cancel the deadline if the object was completed as a result of
        // the deadline being reached. However, setting the canceller to null inside
        // deadlineReached() would introduce a race condition if an object as completed
        // due to another criteria being met at almost the same time as the DeadlineService
        // invokes deadlineReached().
        canceller.cancel();
        canceller = null;
    }

    @Override
    public void deadlineReached(Object context) {
        AsyncCompleter completer = (AsyncCompleter)context;
        completer.asyncComplete();
    }

}
