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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.util.Map;

import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;

public class COSParquetObjectTest extends AbstractTest {
    COSWritableObjectFactory objFactory;
    Map<String,String> localConfMap;
    
    @Before
    public void setUp() {
      localConfMap = testConfigMap();
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_FORMAT,
        COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET
      );
      COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
      objFactory = new COSWritableObjectFactory(cfg);
    }
  
    @Test
    public void testCreateKey() {
        COSParquetObject object = (COSParquetObject) objFactory.create();
        object.put(new SinkRecord("topic", 7, null, null, null, null, 0, null, null, null));
        assertEquals("topic/7/0000000000000000-0000000000000000.parquet", object.createKey());

        object.put(new SinkRecord("topic", 7, null, null, null, null, 1, null, null, null));
        object.put(new SinkRecord("topic", 7, null, null, null, null, 2, null, null, null));
        assertEquals("topic/7/0000000000000000-0000000000000002.parquet", object.createKey());

        object = (COSParquetObject) objFactory.create();
        object.put(new SinkRecord("topic", 111, null, null, null, null, 1234567890123456L, null, null, null));
        object.put(new SinkRecord("topic", 111, null, null, null, null, 1234567890123457L, null, null, null));
        assertEquals("topic/111/1234567890123456-1234567890123457.parquet", object.createKey());
    }

    @Test
    public void testWrite() {
        // load the actual schema
        COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
        SchemaProviderFactory f = new SchemaProviderFactory(cfg);
        SchemaProvider p = f.create();
        Schema schema = p.getSchema();
        AvroData avroData = new AvroData(new AvroDataConfig.Builder()
        .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
        .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1)
        .build()
        );
        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(schema);
        Bucket mockBucket = mock(Bucket.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ByteArrayInputStream> streamCaptor = ArgumentCaptor.forClass(ByteArrayInputStream.class);
        ArgumentCaptor<ObjectMetadata> metadataCaptor = ArgumentCaptor.forClass(ObjectMetadata.class);
        final String key = "some-name";
        GenericRecord rec = new GenericRecordBuilder(schema)
        .set("system", "foo")
        .set("code", "bar")
        .build();
        
        assertEquals(
          "failing schema to/from conversion", 
          schema.getFullName(), 
          avroData.fromConnectSchema(connectSchema).getFullName()
        );
        SchemaAndValue connectData = avroData.toConnectData(schema, rec);
        assertEquals(
          "failing data to/from conversion", 
          ((GenericRecord)rec).get("system"), 
          ((GenericRecord)avroData.fromConnectData(connectData.schema(), connectData.value())).get("system")
        );
        COSParquetObject object = (COSParquetObject) objFactory.create();
        SinkRecord sinkRecord  = new SinkRecord("topic", 7, null, key, connectSchema, connectData.value(), 0, null, null, null);
        final String objectKey = "topic/7/0000000000000000-0000000000000000.parquet";
        object.put(sinkRecord);
        object.write(mockBucket);
        verify(mockBucket).putObject(keyCaptor.capture(), streamCaptor.capture(), metadataCaptor.capture());
        assertEquals(objectKey, keyCaptor.getValue());
        assertEquals(metadataCaptor.getValue().getContentLength(), streamCaptor.getValue().available());
    }
}
