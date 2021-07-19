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

import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;

import static org.junit.Assert.*;

public class AvroConverterTest extends AbstractTest {

  Map<String,String> localConfMap;
  
  @Before
  public void setUp() {
    localConfMap = testConfigMap();
  }

  @Test
  public void configureValidSchema() {
    Path absPath = FileSystems.getDefault()
    .getPath(schemaDir)
    .normalize()
    .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), schemaFile).toUri().toString()
    );
    AvroConverter converter = new AvroConverter();
    converter.configure(localConfMap, false);
  }

  @Test(expected = SchemaParseException.class)
  public void configureInvalidSchema() {
    Path absPath = FileSystems.getDefault()
        .getPath("src/test/resources/badschemas")
        .normalize()
        .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), "foo.json").toUri().toString()
    );
    AvroConverter converter = new AvroConverter();
    converter.configure(localConfMap, false);
  }

  @Test
  public void toConnectDataWithSchema() throws IOException {
    Path absPath = FileSystems.getDefault()
    .getPath(schemaDir)
    .normalize()
    .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), schemaFile).toUri().toString()
    );
    AvroConverter converter = new AvroConverter();
    converter.configure(localConfMap, false);

    SchemaAndValue sav = converter.toConnectData("bogus", getSampleAvroRecord(localConfMap));

    Schema connectSchema = sav.schema();
    assertEquals(Schema.Type.STRUCT, connectSchema.type());
    assertEquals(Schema.Type.STRING, connectSchema.field("system").schema().type());
    assertEquals(Schema.Type.STRING, connectSchema.field("version").schema().type());

    Struct connectRec = (Struct)sav.value();
    assertEquals("foo", connectRec.getString("system"));
    assertEquals("1", connectRec.getString("version"));
  }

  private byte[] getSampleAvroRecord(Map<String,String> props) throws IOException {
    COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(props);
    SchemaProviderFactory f = new SchemaProviderFactory(cfg);
    SchemaProvider p = f.create();
    org.apache.avro.Schema sch = p.getSchema();
    GenericRecord rec = new GenericData.Record(sch);
    rec.put("system", "foo");
    rec.put("code", "bar");
    rec.put("version", "1");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(sch);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Encoder datumEncoder = EncoderFactory.get().binaryEncoder(baos, (BinaryEncoder) null);

    datumWriter.write(rec, datumEncoder);
    datumEncoder.flush();

    return baos.toByteArray();
  }

  private static final byte[] rec = {2, 30, 28, 85, 42, 40, 18, 89, 46, 124, 125, 23, 85, 57, 46, 115, 67, 0, 2, 28, 40, 113, 105, 37, 63, 67, 77, 20, 47, 105, 37, 66, 116, 125, 2, 20, 94, 103, 69, 0, 77, 55, 82, 41, 93, 119, 2, 22, 45, 105, 95, 11, 107, 49, 58, 117, 113, 25, 14};
  @Test
  public void toConnectDataWithSchemaAlt() throws IOException {
    Path absPath = FileSystems.getDefault()
    .getPath(schemaDir)
    .normalize()
    .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), schemaFile).toUri().toString()
    );
    AvroConverter converter = new AvroConverter();
    converter.configure(localConfMap, false);

    SchemaAndValue sav = converter.toConnectData("bogus", rec);

    Schema connectSchema = sav.schema();
    assertEquals(Schema.Type.STRUCT, connectSchema.type());
    assertEquals(Schema.Type.STRING, connectSchema.field("code").schema().type());
    assertEquals(Schema.Type.STRING, connectSchema.field("version").schema().type());

    Struct connectRec = (Struct)sav.value();
    assertNotNull(connectRec.getString("code"));
  }


}
