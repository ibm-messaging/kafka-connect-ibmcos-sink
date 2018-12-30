package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

class OSObject {

    private static final Charset UTF8 = Charset.forName("UTF8");

    private final int recordsPerObject;
    private final List<SinkRecord> records = new LinkedList<>();
    private Long lastOffset;

    OSObject(final int recordsPerObject) {
        this.recordsPerObject = recordsPerObject;
    }


    void put(SinkRecord record) {
        if (ready()) {
            throw new IllegalStateException("Record added to object which is already ready");
        }
        records.add(record);
        lastOffset = record.kafkaOffset();
    }

    boolean ready() {
        return records.size() >= recordsPerObject;
    }

    void write(final Bucket bucket) {
        if (!ready()) {
            throw new IllegalSaslStateException("Attempted to write object before it is ready");
        }

        // TODO: how should records with a zero length value be handled?

        // TODO: this is in-efficient. It would probably be better to convert the SyncRecords into
        //       []byte each time one is received, and implement a gathering input stream.
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (SinkRecord record : records) {
            try {
                baos.write(createValue(record));
            } catch(IOException e) {
                // Ignore, as it shouldn't be possible for a write to a ByteArrayOutputStream to
                // raise this exception.
            }
        }

        final String key = createKey(records.get(0));
        final byte[] value = baos.toByteArray();
        final ByteArrayInputStream bais = new ByteArrayInputStream(value);
        bucket.putObject(key, bais, createMetadata(key, value));
    }

    Long lastOffset() {
        return lastOffset;
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
