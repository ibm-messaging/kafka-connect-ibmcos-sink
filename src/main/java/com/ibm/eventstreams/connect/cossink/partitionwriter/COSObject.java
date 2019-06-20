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
import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

class COSObject {

    private static final Charset UTF8 = Charset.forName("UTF8");
    private static final byte[] EMPTY = new byte[0];

    private final List<SinkRecord> records = new LinkedList<>();
    private Long lastOffset;

    COSObject() {
    }


    void put(SinkRecord record) {
        records.add(record);
        lastOffset = record.kafkaOffset();
    }


    void write(final Bucket bucket) {
        if (records.size() == 0) {
            throw new IllegalStateException("Attempting to write an empty object");
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

        final String key = createKey();
        final byte[] value = baos.toByteArray();
        final ByteArrayInputStream bais = new ByteArrayInputStream(value);
        bucket.putObject(key, bais, createMetadata(key, value));
    }

    Long lastOffset() {
        return lastOffset;
    }

    String createKey() {
        SinkRecord firstRecord = records.get(0);
        return String.format("%s/%d/%016d-%016d",
                firstRecord.topic(), firstRecord.kafkaPartition(), firstRecord.kafkaOffset(), lastOffset);
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
