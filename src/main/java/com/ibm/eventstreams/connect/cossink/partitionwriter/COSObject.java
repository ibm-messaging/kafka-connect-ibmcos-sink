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
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

class COSObject {
    private static final Logger LOG = LoggerFactory.getLogger(COSObject.class);

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final byte[] EMPTY = new byte[0];

    private final List<SinkRecord> records = new LinkedList<>();
    private Long lastOffset;
    private final byte[] recordSeparatorBytes;

    COSObject(String recordDelimiter) {
        if (recordDelimiter != null) {
            LOG.trace("> setting recordDelimiter to: {} of length {}", recordDelimiter, recordDelimiter.length());
            this.recordSeparatorBytes = recordDelimiter.getBytes();
        } else {
            this.recordSeparatorBytes = new byte[0];
        }
    }

    void put(SinkRecord record) {
        LOG.trace("> put, {}-{} offset={}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        records.add(record);
        lastOffset = record.kafkaOffset();
        LOG.trace("< put");
    }

    void write(final Bucket bucket) {
        LOG.trace("> write, records.size={} lastOffset={}", records.size(), lastOffset);
        if (records.isEmpty()) {
            throw new IllegalStateException("Attempting to write an empty object");
        }

        final String key = createKey();
        final byte[] value = createStream();
        final ByteArrayInputStream bais = new ByteArrayInputStream(value);
        bucket.putObject(key, bais, createMetadata(key, value));
        LOG.trace("< write, key={}", key);
    }

    Long lastOffset() {
        return lastOffset;
    }

    String createKey() {
        SinkRecord firstRecord = records.get(0);
        return String.format("%s/%d/%016d-%016d",
                firstRecord.topic(), firstRecord.kafkaPartition(), firstRecord.kafkaOffset(), lastOffset);
    }

    byte[] createStream() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (SinkRecord record : records) {
            try {
                baos.write(createValue(record));
                // will not write any byte unless there is something to write
                baos.write(recordSeparatorBytes);
            } catch(IOException e) {
                // Ignore, as it shouldn't be possible for a write to a ByteArrayOutputStream to
                // raise this exception.
            }
        }
        return baos.toByteArray();
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
            Object value = record.value();
            if (value != null) {
                result = value.toString().getBytes(UTF8);
            } else {
                result = EMPTY;
            }
        }
        return result;
    }

    private static ObjectMetadata createMetadata(final String key, final byte[] value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(value.length);
        return metadata;
    }
}
