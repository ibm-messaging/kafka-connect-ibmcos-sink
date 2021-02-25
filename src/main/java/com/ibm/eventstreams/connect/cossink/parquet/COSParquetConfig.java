package com.ibm.eventstreams.connect.cossink.parquet;

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class COSParquetConfig {

    private String schemaRegistryUrl;
    private String schemaRegistryApiKey;
    private String schemaSubject;
    private int schemaVersion;
    private int schemaCacheSize;
    private boolean enhancedSchemaSupport;
    private int parquetBufferSize;
    private String parquetWriteMode;
    private String parquetCompressionCodec;
    private int parquetRowGroupSize;
    private int parquetPageSize;
    private boolean parquetDictionaryEncoding;

    public static Builder newBuilder() {
        return new Builder();
    }

    private COSParquetConfig() {}

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getSchemaRegistryApiKey() {
        return schemaRegistryApiKey;
    }

    public String getSchemaSubject() {
        return schemaSubject;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public int getSchemaCacheSize() {
        return schemaCacheSize;
    }

    public boolean isEnhancedSchemaSupport() {
        return enhancedSchemaSupport;
    }

    public int getParquetBufferSize() {
        return parquetBufferSize;
    }

    public ParquetFileWriter.Mode getParquetWriteMode() throws Exception {
        switch (parquetWriteMode) {
            case "create":
                return ParquetFileWriter.Mode.CREATE;
            case "overwrite":
                return ParquetFileWriter.Mode.OVERWRITE;
            default:
                throw new Exception("Parquet write mode not found.");
        }
    }

    public CompressionCodecName getParquetCompressionCodec() throws Exception {
        switch (parquetCompressionCodec) {
            case "uncompressed":
                return CompressionCodecName.UNCOMPRESSED;
            case "snappy":
                return CompressionCodecName.SNAPPY;
            case "gzip":
                return CompressionCodecName.GZIP;
            case "lzo":
                return CompressionCodecName.LZO;
            case "brotli":
                return CompressionCodecName.BROTLI;
            case "lz4":
                return CompressionCodecName.LZ4;
            case "zstd":
                return CompressionCodecName.ZSTD;
            default:
                throw new Exception("Compression codec name not found.");
        }
    }

    public int getParquetRowGroupSize() {
        return parquetRowGroupSize;
    }

    public int getParquetPageSize() {
        return parquetPageSize;
    }

    public boolean isParquetDictionaryEncoding() {
        return parquetDictionaryEncoding;
    }

    public static class Builder {

        private COSParquetConfig cosAvroParquetConfig;

        private Builder() {
            cosAvroParquetConfig = new COSParquetConfig();
        }

        public Builder setSchemaRegistryUrl(String schemaRegistryUrl) {
            cosAvroParquetConfig.schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        public Builder setSchemaRegistryApiKey(String schemaRegistryApiKey) {
            cosAvroParquetConfig.schemaRegistryApiKey = schemaRegistryApiKey;
            return this;
        }

        public Builder setSchemaSubject(String schemaSubject) {
            cosAvroParquetConfig.schemaSubject = schemaSubject;
            return this;
        }

        public Builder setSchemaVersion(int schemaVersion) {
            cosAvroParquetConfig.schemaVersion = schemaVersion;
            return this;
        }

        public Builder setSchemaCacheSize(int schemaCacheSize) {
            cosAvroParquetConfig.schemaCacheSize = schemaCacheSize;
            return this;
        }

        public Builder setEnhancedSchemaSupport(boolean enhancedSchemaSupport) {
            cosAvroParquetConfig.enhancedSchemaSupport = enhancedSchemaSupport;
            return this;
        }

        public Builder setParquetBufferSize(int parquetBufferSize) {
            cosAvroParquetConfig.parquetBufferSize = parquetBufferSize;
            return this;
        }

        public Builder setParquetWriteMode(String parquetWriteMode) {
            cosAvroParquetConfig.parquetWriteMode = parquetWriteMode;
            return this;
        }

        public Builder setParquetCompressionCodec(String parquetCompressionCodec) {
            cosAvroParquetConfig.parquetCompressionCodec = parquetCompressionCodec;
            return this;
        }

        public Builder setParquetRowGroupSize(int parquetRowGroupSize) {
            cosAvroParquetConfig.parquetRowGroupSize = parquetRowGroupSize;
            return this;
        }

        public Builder setParquetPageSize(int parquetPageSize) {
            cosAvroParquetConfig.parquetPageSize = parquetPageSize;
            return this;
        }

        public Builder setParquetDictionaryEncoding(boolean parquetDictionaryEncoding) {
            cosAvroParquetConfig.parquetDictionaryEncoding = parquetDictionaryEncoding;
            return this;
        }

        public COSParquetConfig build() {
            return cosAvroParquetConfig;
        }
    }
}
