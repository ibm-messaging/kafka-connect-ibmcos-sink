package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineCanceller;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineListener;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineServiceImpl;

class OSPartitionWriter extends RequestProcessor<RequestType> implements DeadlineListener, PartitionWriter {

    private final int deadlineSec;
    private final int intervalSec;
    private final int recordsPerObject;
    private final Bucket bucket;
    private final DeadlineService deadlineService;

    private OSObject osObject;
    private Long objectCount = 0L;
    private DeadlineCanceller deadlineCancller;

    private AtomicReference<Long> lastOffset = new AtomicReference<>();

    OSPartitionWriter(final int deadlineSec, final int intervalSec, final int recordsPerObject, final Bucket bucket) {
        this(deadlineSec, intervalSec, recordsPerObject, bucket, new DeadlineServiceImpl());
    }

    // Constructor for unit test.
    OSPartitionWriter(final int deadlineSec, final int intervalSec, final int recordsPerObject, final Bucket bucket, final DeadlineService deadlineService) {
        super(RequestType.CLOSE);
        this.deadlineSec = deadlineSec;
        this.intervalSec = intervalSec;
        this.recordsPerObject = recordsPerObject;
        this.bucket = bucket;
        this.deadlineService = deadlineService;
    }

    @Override
    public Long preCommit() {
        Long offset = lastOffset.getAndSet(null);
        if (offset == null) {
            return null;
        }
        // Commit the offset one beyond the last record processed. This is
        // where processing should resume from if the task fails.
        return offset + 1;
    }

    @Override
    public void put(final SinkRecord record) {
        queue(RequestType.PUT, record);
    }

    @Override
    public void close() {
        deadlineService.close();
        super.close();
    }

    private void sync() {
        objectCount++;
        osObject.write(bucket);
        lastOffset.set(osObject.lastOffset());
        osObject = null;
        deadlineCancller = null;
    }

    @Override
    void process(RequestType type, Object context) {
        switch (type) {
        case CLOSE:
            break;
        case PUT:
            final SinkRecord record = (SinkRecord)context;
            boolean accepted = false;

            while(!accepted) {
                if (osObject == null) {
                    osObject = new OSObject(recordsPerObject, intervalSec);
                    if (deadlineSec > 0) {
                        deadlineCancller = deadlineService.schedule(this, deadlineSec, TimeUnit.SECONDS, objectCount);
                    }
                }
                accepted = osObject.offer(record);
                if (osObject.ready()) {
                    if (deadlineCancller != null) {
                        deadlineCancller.cancel();
                    }
                    sync();
                }
            }
            break;
        case DEADLINE:
            final long deadlineObjectCount = (long)context;
            if (deadlineObjectCount == objectCount) {
                sync();
            }
            break;
        }

    }

    @Override
    public void deadlineReached(Object context) {
        queue(RequestType.DEADLINE, context);
    }

}
