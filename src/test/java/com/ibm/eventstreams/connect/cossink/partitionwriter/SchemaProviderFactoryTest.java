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

import static org.junit.Assert.*;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;

import org.apache.avro.SchemaParseException;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests SchemaProviderFactory and FileSchemaProvider along with it.
 * 
 * For the FileSchemaProvider there are sample schemas in the {@code resources/schemas}
 * directory that will be loaded in the correct order (sorted lexicographically).
 */
public class SchemaProviderFactoryTest extends AbstractTest {

  Map<String,String> localConfMap;
  
  @Before
  public void setUp() {
    localConfMap = testConfigMap();
  }

  @Test
  public void testCreateFileProviderSingle() {
    Path absPath = FileSystems.getDefault()
        .getPath(schemaDir)
        .normalize()
        .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), schemaFile).toUri().toString()
    );
    COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
    SchemaProviderFactory f = new SchemaProviderFactory(cfg);
    SchemaProvider p = f.create();
    assertNotNull("Provider not created", p);
    assertNotNull("Schema not created", p.getSchema());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFileProviderParquetNoSchema() {
    localConfMap.put(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_FORMAT, COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET);
    if (localConfMap.containsKey(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI)) {
      localConfMap.remove(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI);
    }
    COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
    SchemaProviderFactory f = new SchemaProviderFactory(cfg);
    SchemaProvider p = f.create();
    assertNotNull("Provider not created", p);
    assertNull("Schema not created", p.getSchema());
  }

  @Test
  public void testCreateFileProviderMulti() {
    Path absPath = FileSystems.getDefault()
        .getPath(schemaDir)
        .normalize()
        .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      absPath.toUri().toString()
    );
    COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
    SchemaProviderFactory f = new SchemaProviderFactory(cfg);
    SchemaProvider p = f.create();
    assertNotNull("Provider not created", p);
    assertNotNull("Schema not created", p.getSchema());
    assertEquals("Incorrect schema:", "org.alvearie.hdtl.model.fhir.AuditEvent", p.getSchema().getFullName());
  }

  @Test(expected = SchemaParseException.class)
  public void testInvalidSchema() {
    Path absPath = FileSystems.getDefault()
        .getPath("src/test/resources/badschemas")
        .normalize()
        .toAbsolutePath();
    localConfMap.put(
      COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
      Paths.get(absPath.toString(), "foo.json").toUri().toString()
    );
    COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
    SchemaProviderFactory f = new SchemaProviderFactory(cfg);
    SchemaProvider p = f.create();
    assertNotNull("Provider not created", p);
    assertNull("Schema not expected", p.getSchema());
  }


}
