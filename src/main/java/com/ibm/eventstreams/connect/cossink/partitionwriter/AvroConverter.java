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
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of Converter that uses Avro schemas and objects without
 * using an external schema registry. Requires that a `schema.path` configuration
 * option is provided that tells the converter where to find its Avro schema.
 *
 * @author Bill Li
 */
public class AvroConverter implements Converter {

  private static final Logger LOG = LoggerFactory.getLogger(AvroConverter.class);

  private static final int SCHEMA_CACHE_SZ = 1;

    private org.apache.avro.Schema avroSchema = null;
    private AvroData avroDataHelper = null;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

      LOG.debug("Configuration properties -> {}", configs);
      this.avroDataHelper = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, SCHEMA_CACHE_SZ)
        .build()
      );
      if (!configs.containsKey(AvroConverterConfig.CONFIG_NAME_VALUE_CONVERTER_SCHEMA_URI)) {
        throw new IllegalArgumentException("Missing required property: "+AvroConverterConfig.CONFIG_NAME_VALUE_CONVERTER_SCHEMA_URI);
      }
      SchemaProvider schemaProvider = new SchemaProviderFactory(new AvroConverterConfig(configs))
          .create();
      this.avroSchema = schemaProvider.getSchema();
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
      throw new NotImplementedException("Method is not implemented");
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
      LOG.trace("> toConnectData");
      DatumReader<GenericRecord> datumReader;
      if (avroSchema != null) {
        LOG.debug("Convert value using schema {}", avroSchema.getFullName());
        datumReader = new GenericDatumReader<>(avroSchema);
      } else {
        LOG.debug("Convert value without schema");
        datumReader = new GenericDatumReader<>();
      }
      GenericRecord datum = null;

      try {
        LOG.debug("value length {}", value.length);
        SeekableByteArrayInput sbai = new SeekableByteArrayInput(value);
        Decoder datumDecoder = DecoderFactory.get().binaryDecoder(sbai, (BinaryDecoder) null);
        datum = datumReader.read(null, datumDecoder);
        if (datum == null) {
          throw new NullPointerException("GenericRecord instance is null.");
        }

        if (avroSchema != null) {
          LOG.trace("< toConnectData return with external schema");
          return avroDataHelper.toConnectData(avroSchema, datum);
        } else {
          LOG.trace("< toConnectData return with schema from message");
          return avroDataHelper.toConnectData(datum.getSchema(), datum);
        }
      } catch (IOException ioe) {
        throw new DataException("Failed to deserialize Avro data from topic " + topic, ioe);
      }
    }

  public class AvroConverterConfig extends AbstractConfig {

    private static final String CONFIG_GROUP = "cos";
    public static final String CONFIG_NAME_VALUE_CONVERTER_SCHEMA_URI = "cos.writer.schema.uri";
    private static final String CONFIG_DOCUMENTATION_VALUE_CONVERTER_SCHEMA_URI =
            "URI of the record schema, in Avro format, required for Avro value converter. Currently only file URIs are supported.";
    private static final String CONFIG_DISPLAY_VALUE_CONVERTER_SCHEMA_URI = "Avro schema URI";

    public AvroConverterConfig(ConfigDef definition, Map<?, ?> originals) {
      super(definition, originals);
    }

    public AvroConverterConfig(Map<String, ?> parsedConfig) {
        this(
          new ConfigDef()
              .define(CONFIG_NAME_VALUE_CONVERTER_SCHEMA_URI, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
              CONFIG_DOCUMENTATION_VALUE_CONVERTER_SCHEMA_URI, CONFIG_GROUP, 1, Width.MEDIUM,
              CONFIG_DISPLAY_VALUE_CONVERTER_SCHEMA_URI), 
          parsedConfig
        );
    }

  }
}
