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

import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;

import org.apache.avro.Schema;
import org.apache.kafka.common.config.AbstractConfig;

/**
 * Creates {@link WritableObject} instances that write to IBM Cloud Object Store.
 */
public class COSWritableObjectFactory implements WritableObjectFactory<COSObject> {

    private final boolean isParquet;
    private final boolean useDelimiters;
    private final ParquetRecordWriterProvider rwProvider;
    private final Schema schema;

    /**
     * Constructor.
     * @param config Connector configuration object.
     */
    public COSWritableObjectFactory(AbstractConfig config) {
      this.useDelimiters = config.getBoolean(COSSinkConnectorConfig.CONFIG_NAME_COS_OBJECT_RECORD_DELIMITER_NL);
      if (config.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_FORMAT).equals(COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET)) {
        if (config.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI) == null) {
          throw new IllegalArgumentException(
            String.format("%s is required for Parquet output",COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI)
          );
        }
        this.isParquet = true;
        SchemaProvider schemaProvider = new SchemaProviderFactory(config).create();
        this.schema = schemaProvider.getSchema();
        this.rwProvider = new ParquetRecordWriterProvider(config, this.schema);
        }
      else {
        this.isParquet = false;
        this.schema = null;
        this.rwProvider = null;
      }
    }

    @Override
    public COSObject create() {
      if (this.isParquet) {
        return new COSParquetObject(rwProvider);
      }
      else {
        return new COSObject(this.useDelimiters);
      }
    }
}
