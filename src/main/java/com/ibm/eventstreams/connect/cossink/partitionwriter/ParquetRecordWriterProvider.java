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

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

/**
 * A provider of ParquetRecordWriters used by COSParquetObject to write buffered records.
 */
public class ParquetRecordWriterProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";

  private final Schema schema;
  private final AvroData avro;
  private final int bufferSize;
  private final Mode mode;
  private final CompressionCodecName compressionCodec;
  private final int rowGroupSize;
  private final int pageSize;
  private final boolean enableDictionary;


  /**
   * Constructor.
   * @param connectorConfig configuration object
   * @param schema Avro schema to use for record transformation
   */
  public ParquetRecordWriterProvider(AbstractConfig connectorConfig, Schema schema) {
    this.schema = schema;
    this.bufferSize = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_BUFFER_SIZE);
    this.rowGroupSize = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_ROW_GROUP_SIZE);
    this.pageSize = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_PAGE_SIZE);
    this.enableDictionary = connectorConfig.getBoolean(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_ENABLE_DICTIONARY);
    switch (connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_WRITE_MODE)) {
      case COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_CREATE: 
        this.mode = Mode.CREATE;
        break;
      case COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_OVERWRITE: 
        this.mode = Mode.OVERWRITE;
        break;
      default: 
        LOG.warn("Unexpected file write mode {}, will default to {}", 
        connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_WRITE_MODE), 
        COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_CREATE
        );
        this.mode = Mode.CREATE;
        break;
    }
    this.compressionCodec = CompressionCodecName.fromConf(
      connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_PARQUET_COMPRESSION_CODEC)
    );
    this.avro = new AvroData(new AvroDataConfig.Builder()
      .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
      .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
      .build()
    );
  }

  public String getExtension() {
      return EXTENSION;
  }

  /**
   * Create a record writer for the given file.
   * 
   * <p>It can throw an unchecked {@link org.apache.kafka.connect.errors.ConnectException} if 
   * an underlying {@code IOException} is caught.
   * 
   * @param cosAvroParquetConfig: Configuration parameters to write parquet output
   * @param filename: File name generated from createKeys() in COSObject
   * @return ParquetRecordWriter that produces Parquet output
   */
  public ParquetRecordWriter getRecordWriter(Bucket bucket, String filename) {
    try {
      OutputFile outFile = new COSOutputFile(bucket, filename, this.bufferSize);

      ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
        .<GenericRecord>builder(outFile)
        .withSchema(schema)
        .withWriteMode(this.mode)
        .withCompressionCodec(this.compressionCodec)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withDictionaryEncoding(enableDictionary)
        .build(); 
      return new ParquetRecordWriter(parquetWriter, this.avro);
    }
    catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}
