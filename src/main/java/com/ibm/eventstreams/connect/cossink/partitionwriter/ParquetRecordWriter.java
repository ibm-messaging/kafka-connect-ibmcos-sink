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

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;

/**
 * An implementation of {@link io.confluent.connect.storage.format.RecordWriter} that
 * writes records as a Parquet object to COS
 */
class ParquetRecordWriter implements RecordWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordWriter.class);
  private final ParquetWriter<GenericRecord> parquetWriter;
  private final AvroData avroHelper;

  /**
   * Constructor.
   * @param writer the writer that does conversion of generic records into the 
   *        Parquet format.
   */
  ParquetRecordWriter(ParquetWriter<GenericRecord> writer, AvroData avroHelper) {
    this.parquetWriter = writer; 
    this.avroHelper = avroHelper;
  }

  /**
   * Write record as Parquet to COS
   * @param record record to be written to COS (format: SinkRecord)
   */
  @Override
  public void write(SinkRecord record) {
      LOG.debug("Sink record: {}", record);
      Object value = avroHelper.fromConnectData(record.valueSchema(), record.value());
      try {
          parquetWriter.write((GenericRecord) value);
      } catch (IOException e) {
          throw new ConnectException(e);
      }
  }

  /**
   * Called when commit method is executed
   */
  @Override
  public void close() {
      try {
        commit();
        if (parquetWriter != null) {
          parquetWriter.close();
        }
      } catch (IOException e) {
          throw new ConnectException(e);
      }
  }

  /**
   * Called when write operation completes
   */
  @Override
  public void commit() {
      LOG.trace("> commit");
  }
}
