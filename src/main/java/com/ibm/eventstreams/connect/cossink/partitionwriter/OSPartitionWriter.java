package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

class OSPartitionWriter implements PartitionWriter {

    private static final Charset UTF8 = Charset.forName("UTF8");

    private final Bucket bucket;
    private Long lastOffset;

    OSPartitionWriter(final Bucket bucket) {
        this.bucket = bucket;
    }

    @Override
    public Long preCommit() {
        final Long result = lastOffset;
        lastOffset = null;
        return result;
    }

    @Override
    public void put(final SinkRecord record) {
        final String key = createKey(record);
        final byte[] value = createValue(record);
        final ObjectMetadata metadata = createMetadata(key, value);

        bucket.putObject(key, new ByteArrayInputStream(value), metadata);
        lastOffset = record.kafkaOffset();
    }

    @Override
    public void close() {
        // Currently a no-op.
    }

    private static String createKey(final SinkRecord record) {
        return String.format("%4d-%d", record.kafkaPartition(), record.kafkaOffset());
    }

    private static byte[] createValue(final SinkRecord record) {
        final Schema schema = record.valueSchema();
        byte[] result = null;
        if (schema == null || schema.type() == Type.BYTES) {
            if (record.value() instanceof byte[]) {
                result = (byte[])record.value();
            } else if (record.value() instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer)record.value();
                result = new byte[bb.remaining()];
                bb.get(result);
            }
        }

        if (result == null) {
            result = record.value().toString().getBytes(UTF8);
        }
        return result;
    }

    private static ObjectMetadata createMetadata(final String key, final byte[] value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(value.length);
        return metadata;
    }
}
