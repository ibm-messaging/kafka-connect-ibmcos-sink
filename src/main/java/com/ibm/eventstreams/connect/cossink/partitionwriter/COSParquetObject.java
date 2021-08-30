/*
 * Copyright 2021 IBM Corporation
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

import com.ibm.cos.Bucket;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a WritableObject instance capable of writing Parquet files.
 */
public class COSParquetObject extends COSObject {
  
  private static final Logger LOG = LoggerFactory.getLogger(COSParquetObject.class);
  private ParquetRecordWriterProvider recWriterProvider;

  public COSParquetObject(ParquetRecordWriterProvider recWriterProvider) {
    super(false);
    this.recWriterProvider = recWriterProvider;
  }

  @Override
  String createKey() {
    return super.createKey() + this.recWriterProvider.getExtension();
  }

  @Override
  public void write(final Bucket bucket) {
    LOG.trace("> write, records.size={} lastOffset={}", records.size(), lastOffset);
    if (records.isEmpty()) {
        throw new IllegalStateException("Attempting to write an empty object");
    }

    final String key = createKey();
    ParquetRecordWriter writer = recWriterProvider.getRecordWriter(bucket, key);
    for (SinkRecord record : records) {
      writer.write(record);
    }
    writer.commit();
    writer.close();
    LOG.trace("< write, key={}", key);
  }



}
