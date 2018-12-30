package com.ibm.eventstreams.connect.cossink.partitionwriter;

import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.cos.Bucket;

class OSPartitionWriter implements PartitionWriter {

    private final Bucket bucket;
    private final int recordsPerObject;
    private OSObject osObject;

    private Long lastOffset;

    OSPartitionWriter(final int recordsPerObject, final Bucket bucket) {
        this.recordsPerObject = recordsPerObject;
        this.bucket = bucket;
    }

    @Override
    public Long preCommit() {
        if (lastOffset == null) {
            return null;
        }
        // Commit the offset one beyond the last record processed. This is
        // where processing should resume from if the task fails.
        return lastOffset + 1;
    }

    @Override
    public void put(final SinkRecord record) {
        if (osObject == null) {
            osObject = new OSObject(recordsPerObject);
        }
        osObject.put(record);
        if (osObject.ready()) {
            osObject.write(bucket);
            lastOffset = osObject.lastOffset() + 1;
            osObject = null;
        }
    }

    @Override
    public void close() {
        // Currently a no-op.
    }
}
