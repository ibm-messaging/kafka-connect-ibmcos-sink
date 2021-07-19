/*
 * Copyright 2019, 2021 IBM Corporation
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
package com.ibm.eventstreams.connect.cossink;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class COSSinkConnectorConfig extends AbstractConfig {

    private static final String CONFIG_GROUP_COS = "cos";

    public static final String CONFIG_NAME_COS_API_KEY = "cos.api.key";
    private static final String CONFIG_DOCUMENTATION_COS_API_KEY =
            "API key for connecting to the Cloud Object Storage instance.";
    private static final String CONFIG_DISPLAY_COS_API_KEY = "API key";

    public static final String CONFIG_NAME_COS_SERVICE_CRN = "cos.service.crn";
    private static final String CONFIG_DOCUMENTATION_COS_SERVICE_CRN =
            "Service CRN for the Cloud Object Storage instance.";
    private static final String CONFIG_DISPLAY_COS_SERVICE_CRN  = "Service CRN";

    public static final String CONFIG_NAME_COS_BUCKET_LOCATION = "cos.bucket.location";
    private static final String CONFIG_DOCUMENTATION_COS_BUCKET_LOCATION =
            "Location of the Cloud Object Storage bucket, for example: eu-gb.";
    private static final String CONFIG_DISPLAY_COS_BUCKET_LOCATION = "Bucket location";

    public static final String CONFIG_NAME_COS_BUCKET_NAME = "cos.bucket.name";
    private static final String CONFIG_DOCUMENTATION_COS_BUCKET_NAME =
            "Name of the Cloud Object Storage bucket.";
    private static final String CONFIG_DISPLAY_COS_BUCKET_NAME = "Bucket name";

    public static final String CONFIG_NAME_COS_BUCKET_RESILIENCY = "cos.bucket.resiliency";
    private static final String CONFIG_DOCUMENTATION_COS_BUCKET_RESILIENCY =
            "Resiliency of the Cloud Object Storage bucket, for example: cross-region, regional, or single-site.";
    private static final String CONFIG_DISPLAY_COS_BUCKET_RESILIENCY = "Bucket resiliency";
    public static final String CONFIG_VALUE_COS_BUCKET_RESILIENCY_CROSS_REGION = "cross-region";
    public static final String CONFIG_VALUE_COS_BUCKET_RESILIENCY_SINGLE_SITE = "single-site";
    public static final String CONFIG_VALUE_COS_BUCKET_RESILIENCY_REGIONAL = "regional";

    public static final String CONFIG_NAME_COS_ENDPOINT_VISIBILITY = "cos.endpoint.visibility";
    private static final String CONFIG_DOCUMENTATION_COS_ENDPOINT_VISIBILITY =
            "Specify 'public' to connect to the Cloud Object Storage across the public internet, or 'private' to connect " +
            "using the SoftLayer network.";
    private static final String CONFIG_DISPLAY_COS_ENDPOINT_VISIBILITY = "Endpoint visibility";
    public static final String CONFIG_VALUE_COS_ENDPOINT_VISIBILITY_PUBLIC = "public";
    public static final String CONFIG_VALUE_COS_ENDPOINT_VISIBILITY_PRIVATE = "private";

    public static final String CONFIG_NAME_COS_OBJECT_RECORDS = "cos.object.records";

    private static final String CONFIG_DOCUMENTATION_COS_OBJECT_RECORDS =
            "The maximum number of Kafka records to group together into a single object storage object.";
    private static final String CONFIG_DISPLAY_COS_OBJECT_RECORDS = "Records per object";

    public static final String CONFIG_NAME_COS_OBJECT_RECORD_DELIMITER_NL = "cos.object.record.delimiter.nl";
    private static final String CONFIG_DOCUMENTATION_COS_OBJECT_RECORD_DELIMITER_NL =
            "Delimit records with new lines within a single object storage object.";
    private static final String CONFIG_DISPLAY_COS_OBJECT_RECORD_DELIMITER_NL = "Delimit records with new line";

    public static final String CONFIG_NAME_COS_OBJECT_DEADLINE_SECONDS = "cos.object.deadline.seconds";
    private static final String CONFIG_DOCUMENTATION_COS_OBJECT_DEADLINE_SECONDS =
            "The maximum period of (wall clock) time between the connector receiving a Kafka record and the " +
            "connector writing all of the Kafka records it has received so far into an Cloud Object Storage object.";
    private static final String CONFIG_DISPLAY_COS_OBJECT_DEADLINE_SECONDS = "Object deadline seconds";

    public static final String CONFIG_NAME_COS_OBJECT_INTERVAL_SECONDS = "cos.object.interval.seconds";
    private static final String CONFIG_DOCUMENTATION_COS_OBJECT_INTERVAL_SECONDS =
            "The maximum interval (based on Kafka record timestamp) between the first Kafka record to write into an " +
            "object and the last.";
    private static final String CONFIG_DISPLAY_COS_OBJECT_INTERVAL_SECONDS = "Object interval seconds";

    public static final String CONFIG_NAME_COS_ENDPOINTS_URL = "cos.endpoints.url";
    private static final String CONFIG_DOCUMENTATION_COS_ENDPOINTS_URL = "Endpoints URL for the Cloud Object Storage instance. Only set this in environments where a non-default set of endpoints is required.";
    private static final String CONFIG_DISPLAY_COS_ENDPOINTS_URL = "Endpoints URL";
    private static final String CONFIG_VALUE_COS_ENDPOINTS_URL = "https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints";

    public static final String CONFIG_NAME_COS_WRITER_FORMAT = "cos.writer.format";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_FORMAT =
            "Output file format: \"json\" (default) or \"parquet\".";
    private static final String CONFIG_DISPLAY_COS_WRITER_FORMAT = "Output file format";
    public static final String CONFIG_VALUE_COS_WRITER_FORMAT_JSON = "json";
    public static final String CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET = "parquet";

    public static final String CONFIG_NAME_COS_WRITER_SCHEMA_URI = "cos.writer.schema.uri";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_SCHEMA_URI =
            "URI of the record schema, in Avro format, required for writing Parquet output. Currently only file URIs are supported.";
    private static final String CONFIG_DISPLAY_COS_WRITER_SCHEMA_URI = "Avro schema URI";

    public static final String CONFIG_NAME_COS_WRITER_PARQUET_BUFFER_SIZE = "cos.writer.parquet.buffer.size";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_BUFFER_SIZE =
            "Size, in bytes, of the output stream buffer for writing parquet files.";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_BUFFER_SIZE = "Parquet output buffer size";

    // TODO may not be needed
    public static final String CONFIG_NAME_COS_WRITER_PARQUET_WRITE_MODE = "cos.writer.parquet.write.mode";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_WRITE_MODE =
            "Write mode for Parquet files: \"create\" or \"overwrite\".";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_WRITE_MODE = "Parquet file write mode";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_CREATE = "create";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_OVERWRITE = "overwrite";

    public static final String CONFIG_NAME_COS_WRITER_PARQUET_COMPRESSION_CODEC = "cos.writer.parquet.compression.codec";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_COMPRESSION_CODEC =
            "Compression codec for Parquet output. Acceptable values: \"none\", \"uncompressed\", \"snappy\", \"gzip\", \"lzo\", \"brotli\", \"lz4\", \"zstd\"";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_COMPRESSION_CODEC = "Parquet compression codec";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_NONE = "none";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_UNCOMPRESSED = "uncompressed";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_SNAPPY = "snappy";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_GZIP = "gzip";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_LZO = "lzo";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_BROTLI = "brotli";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_LZ4 = "lz4";
    public static final String CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_ZSTD = "zstd";

    public static final String CONFIG_NAME_COS_WRITER_PARQUET_ROW_GROUP_SIZE = "cos.writer.parquet.row.group.size";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_ROW_GROUP_SIZE =
            "Row group size, in bytes, for Parquet files.";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_ROW_GROUP_SIZE = "Parquet row group size";

    public static final String CONFIG_NAME_COS_WRITER_PARQUET_PAGE_SIZE = "cos.writer.parquet.page.size";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_PAGE_SIZE =
            "Parquet file page size, in bytes.";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_PAGE_SIZE = "Parquet file page size";

    public static final String CONFIG_NAME_COS_WRITER_PARQUET_ENABLE_DICTIONARY = "cos.writer.parquet.enable.dictionary";
    private static final String CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_ENABLE_DICTIONARY =
            "Enable Parquet dictionary.";
    private static final String CONFIG_DISPLAY_COS_WRITER_PARQUET_ENABLE_DICTIONARY = "Enable Parquet dictionary";
    
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONFIG_NAME_COS_API_KEY, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_API_KEY, CONFIG_GROUP_COS, 1, Width.MEDIUM,
                CONFIG_DISPLAY_COS_API_KEY)

        .define(CONFIG_NAME_COS_SERVICE_CRN, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_SERVICE_CRN, CONFIG_GROUP_COS, 2, Width.MEDIUM,
                CONFIG_DISPLAY_COS_SERVICE_CRN)

        .define(CONFIG_NAME_COS_BUCKET_LOCATION, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_BUCKET_LOCATION, CONFIG_GROUP_COS, 3, Width.MEDIUM,
                CONFIG_DISPLAY_COS_BUCKET_LOCATION)

        .define(CONFIG_NAME_COS_BUCKET_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_BUCKET_NAME, CONFIG_GROUP_COS, 4, Width.MEDIUM,
                CONFIG_DISPLAY_COS_BUCKET_NAME)

        .define(CONFIG_NAME_COS_BUCKET_RESILIENCY, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(CONFIG_VALUE_COS_BUCKET_RESILIENCY_CROSS_REGION,
                        CONFIG_VALUE_COS_BUCKET_RESILIENCY_SINGLE_SITE,
                        CONFIG_VALUE_COS_BUCKET_RESILIENCY_REGIONAL),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_BUCKET_RESILIENCY, CONFIG_GROUP_COS, 5, Width.MEDIUM,
                CONFIG_DISPLAY_COS_BUCKET_RESILIENCY)

        .define(CONFIG_NAME_COS_ENDPOINT_VISIBILITY, Type.STRING, CONFIG_VALUE_COS_ENDPOINT_VISIBILITY_PUBLIC,
                ConfigDef.ValidString.in(CONFIG_VALUE_COS_ENDPOINT_VISIBILITY_PRIVATE,
                        CONFIG_VALUE_COS_ENDPOINT_VISIBILITY_PUBLIC),
                Importance.LOW,
                CONFIG_DOCUMENTATION_COS_ENDPOINT_VISIBILITY, CONFIG_GROUP_COS, 6, Width.MEDIUM,
                CONFIG_DISPLAY_COS_ENDPOINT_VISIBILITY)

        .define(CONFIG_NAME_COS_OBJECT_RECORDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_OBJECT_RECORDS, CONFIG_GROUP_COS, 7, Width.MEDIUM,
                CONFIG_DISPLAY_COS_OBJECT_RECORDS)

        .define(CONFIG_NAME_COS_OBJECT_RECORD_DELIMITER_NL, Type.BOOLEAN, false, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_COS_OBJECT_RECORD_DELIMITER_NL, CONFIG_GROUP_COS, 8, Width.MEDIUM,
                CONFIG_DISPLAY_COS_OBJECT_RECORD_DELIMITER_NL)

        .define(CONFIG_NAME_COS_OBJECT_DEADLINE_SECONDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_OBJECT_DEADLINE_SECONDS, CONFIG_GROUP_COS, 9, Width.MEDIUM,
                CONFIG_DISPLAY_COS_OBJECT_DEADLINE_SECONDS)

        .define(CONFIG_NAME_COS_OBJECT_INTERVAL_SECONDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_OBJECT_INTERVAL_SECONDS, CONFIG_GROUP_COS, 10, Width.MEDIUM,
                CONFIG_DISPLAY_COS_OBJECT_INTERVAL_SECONDS)

        .define(CONFIG_NAME_COS_ENDPOINTS_URL, Type.STRING, CONFIG_VALUE_COS_ENDPOINTS_URL, Importance.LOW,
                CONFIG_DOCUMENTATION_COS_ENDPOINTS_URL, CONFIG_GROUP_COS, 11, Width.MEDIUM,
                CONFIG_DISPLAY_COS_ENDPOINTS_URL)
                .define(CONFIG_NAME_COS_WRITER_FORMAT, Type.STRING, CONFIG_VALUE_COS_WRITER_FORMAT_JSON, 
                ConfigDef.ValidString.in(CONFIG_VALUE_COS_WRITER_FORMAT_JSON, CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET),
                Importance.HIGH, CONFIG_DOCUMENTATION_COS_WRITER_FORMAT, CONFIG_GROUP_COS, 1, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_FORMAT)

        .define(CONFIG_NAME_COS_WRITER_SCHEMA_URI, Type.STRING, null, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_WRITER_SCHEMA_URI, CONFIG_GROUP_COS, 2, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_SCHEMA_URI)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_BUFFER_SIZE, Type.INT, 26214400, Importance.HIGH,
                CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_BUFFER_SIZE, CONFIG_GROUP_COS, 3, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_BUFFER_SIZE)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_WRITE_MODE, Type.STRING, CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_CREATE, 
                ConfigDef.ValidString.in(
                  CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_CREATE, CONFIG_VALUE_COS_WRITER_PARQUET_WRITE_MODE_OVERWRITE),
                Importance.HIGH, CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_WRITE_MODE, CONFIG_GROUP_COS, 4, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_WRITE_MODE)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_COMPRESSION_CODEC, Type.STRING, CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_SNAPPY, 
                ConfigDef.ValidString.in(
                  CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_NONE, CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_UNCOMPRESSED,
                  CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_SNAPPY, CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_GZIP, 
                  CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_LZO, CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_BROTLI, 
                  CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_LZ4, CONFIG_VALUE_COS_WRITER_PARQUET_COMPRESSION_CODEC_ZSTD
                ),
                Importance.HIGH, CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_COMPRESSION_CODEC, CONFIG_GROUP_COS, 5, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_COMPRESSION_CODEC)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_ROW_GROUP_SIZE, Type.INT, 536870912, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_ROW_GROUP_SIZE, CONFIG_GROUP_COS, 6, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_ROW_GROUP_SIZE)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_PAGE_SIZE, Type.INT, 65536, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_PAGE_SIZE, CONFIG_GROUP_COS, 7, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_PAGE_SIZE)

        .define(CONFIG_NAME_COS_WRITER_PARQUET_ENABLE_DICTIONARY, Type.BOOLEAN, true, Importance.MEDIUM,
                CONFIG_DOCUMENTATION_COS_WRITER_PARQUET_ENABLE_DICTIONARY, CONFIG_GROUP_COS, 8, Width.MEDIUM,
                CONFIG_DISPLAY_COS_WRITER_PARQUET_ENABLE_DICTIONARY);

    public COSSinkConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public COSSinkConnectorConfig(Map<String, ?> parsedConfig) {
        this(CONFIG_DEF, parsedConfig);
    }

}
