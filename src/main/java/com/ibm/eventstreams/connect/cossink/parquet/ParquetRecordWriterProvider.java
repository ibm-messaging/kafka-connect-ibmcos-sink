/*
 * IBM Confidential Source Materials
 * (C) Copyright IBM Corp. 2020
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 *
 */
package com.ibm.eventstreams.connect.cossink.parquet;

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.registry.CachedSchemaRegistryClient;
import com.ibm.eventstreams.connect.cossink.registry.SchemaFileClient;
import com.ibm.eventstreams.connect.cossink.registry.SchemaRegistryClient;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class prepares required configuration before writing Parquet to COS.
 * It is instantiated inside COSObject class and called from method writeParquet().
 *
 * @author Bill Li
 */
public class ParquetRecordWriterProvider implements RecordWriterProvider<COSParquetConfig> {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
    private static final String EXTENSION = ".parquet";
    private final Bucket bucket;
    private final AvroData avroDataHelper;

    /**
     * Constructor for ParquetRecordWriterProvider
     * @param bucket COS bucket object for writing parquet output
     * @param avroData Confluent utility class for converting Connect data to Parquet data
     */
    public ParquetRecordWriterProvider(Bucket bucket, AvroData avroData) {
        this.bucket = bucket;
        this.avroDataHelper = avroData;
    }

    /**
     * Returns Parquet Extension
     * @return '.parquet' to append to all file names
     */
    @Override
    public String getExtension() {
        return EXTENSION;
    }

    /**
     * Define Parquet output format and write Parquet output
     * @param cosAvroParquetConfig: Configuration parameters to write parquet output
     * @param filename: File name generated from createKeys() in COSObject
     * @return Record Writer that writes parquet output
     */
    @Override
    public RecordWriter getRecordWriter(COSParquetConfig cosParquetConfig, String filename) {

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(
                cosParquetConfig.getSchemaRegistryUrl(),
                cosParquetConfig.getSchemaRegistryApiKey()
        );
        org.apache.avro.Schema avroSchema = schemaRegistryClient.getBySubject(cosParquetConfig.getSchemaSubject());

        return new RecordWriter() {
            Schema connectSchema = null;
            ParquetWriter<GenericRecord> writer;
            COSParquetOutputFile cosParquetOutputFile;

            /**
             * Write record as Parquet to COS
             * @param record record to be written to COS (format: SinkRecord)
             */
            @Override
            public void write(SinkRecord record) {
                if (connectSchema == null) {
                    connectSchema = record.valueSchema();
                }

                try {
                    String fileFullName = filename
                            + cosParquetConfig.getParquetCompressionCodec().getExtension()
                            + getExtension();

                    cosParquetOutputFile = new COSParquetOutputFile(bucket,
                            fileFullName, cosParquetConfig.getParquetBufferSize());

                    writer = AvroParquetWriter
                            .<GenericRecord>builder(cosParquetOutputFile)
                            .withSchema(avroSchema)
                            .withWriteMode(cosParquetConfig.getParquetWriteMode())
                            .withCompressionCodec(cosParquetConfig.getParquetCompressionCodec())
                            .withRowGroupSize(cosParquetConfig.getParquetRowGroupSize())
                            .withPageSize(cosParquetConfig.getParquetPageSize())
                            .withDictionaryEncoding(cosParquetConfig.isParquetDictionaryEncoding())
                            .build();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }

                LOG.trace("Sink record: {}", record.toString());
                Object value = avroDataHelper.fromConnectData(connectSchema, record.value());
                try {
                    writer.write((GenericRecord) value);
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
                    writer.close();
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }

            /**
             * Called when write operation completes
             */
            @Override
            public void commit() {
                try {
                    cosParquetOutputFile.getCosOut().setCommit();
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException e) {
                    throw new ConnectException(e);
                }
            }
        };
    }
}
