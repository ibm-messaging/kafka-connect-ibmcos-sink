/*
 * Copyright 2019, 2021 IBM Corporation
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
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.completion.AsyncCompleter;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;
import com.ibm.eventstreams.connect.cossink.completion.FirstResult;

class COSPartitionWriter extends RequestProcessor<RequestType> implements PartitionWriter {

    private static final Logger LOG = LoggerFactory.getLogger(COSPartitionWriter.class);

    private final Bucket bucket;
    private final CompletionCriteriaSet completionCriteria;
    private AbstractConfig config = null;
    private COSWritableObjectFactory objectFactory = null;

    private COSObject osObject;
    private Long objectCount = 0L;

    private AtomicReference<Long> lastOffset = new AtomicReference<>();

    /**
     * Constructor with extra properties passed using the config object
     * 
     * @param bucket COS bucket to write to.
     * @param completionCriteria object completion criteria. Determines the batch size.
     * @param delimitRecords if true, written objects will be delimited by new line characters.
     * @param config AbstractConfig implementation carrying parameters for a specific 
     *        type of object to be written.
     */
    COSPartitionWriter(final Bucket bucket, final CompletionCriteriaSet completionCriteria, final AbstractConfig config) {
        super(RequestType.CLOSE);
        this.bucket = bucket;
        this.completionCriteria = completionCriteria;
        this.config = config;
        this.objectFactory = new COSWritableObjectFactory(this.config);
    }

    @Override
    public Long preCommit() {
        LOG.trace("> preCommit");
        Long offset = lastOffset.getAndSet(null);
        if (offset == null) {
            return null;
        }
        // Commit the offset one beyond the last record processed. This is
        // where processing should resume from if the task fails.
        Long retval = offset + 1;
        LOG.trace("< preCommit, retval={}", retval);
        return retval;
    }

    @Override
    public void put(final SinkRecord record) {
        LOG.trace("> put, {}-{} offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        queue(RequestType.PUT, record);
        LOG.trace("< put");
    }

    @Override
    public void close() {
        LOG.trace("> close");
        super.close();
        LOG.trace("< close");
    }

    private void writeObject() {
        objectCount++;
        osObject.write(bucket);
        lastOffset.set(osObject.lastOffset());
        osObject = null;
        completionCriteria.complete();
    }

    private void startObject(SinkRecord record) {
        osObject = this.objectFactory.create();
        osObject.put(record);
        FirstResult result = completionCriteria.first(record, new AsyncCompleterImpl(this, objectCount));
        if (result == FirstResult.COMPLETE) {
            writeObject();
        }
    }

    private boolean haveStartedObject() {
        return osObject != null;
    }

    private void addToExistingObject(SinkRecord record) {
        osObject.put(record);
    }

    @Override
    void process(RequestType type, Object context) {
        switch (type) {
        case CLOSE:
            break;

        case PUT:
            final SinkRecord record = (SinkRecord)context;
            if (haveStartedObject()) {
                switch(completionCriteria.next(record)) {
                case COMPLETE_INCLUSIVE:
                    addToExistingObject(record);
                    writeObject();
                    break;
                case COMPLETE_NON_INCLUSIVE:
                    writeObject();
                    startObject(record);
                    break;
                case INCOMPLETE:
                    addToExistingObject(record);
                    break;
                }
            } else {
                startObject(record);
            }
            break;

        case DEADLINE:
            final long deadlineObjectCount = (long)context;
            if (deadlineObjectCount == objectCount) {
                writeObject();
            }
            break;
        }
    }

    static class AsyncCompleterImpl implements AsyncCompleter {

        private final COSPartitionWriter writer;
        private final long objectCount;

        AsyncCompleterImpl(COSPartitionWriter writer, long objectCount) {
            this.writer = writer;
            this.objectCount = objectCount;
        }

        @Override
        public void asyncComplete() {
            writer.queue(RequestType.DEADLINE, this.objectCount);
        }

    }

}
