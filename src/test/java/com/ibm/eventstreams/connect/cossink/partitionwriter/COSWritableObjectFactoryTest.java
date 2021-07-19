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

import org.junit.Before;
import org.junit.Test;

/**
 * Tests COSWritableObjectFactory.
 * 
 * Will implicitly call FileSchemaProvider, for which there should be 
 * sample schemas in the {@code resources/schemas} directory.
 */
public class COSWritableObjectFactoryTest extends AbstractTest {

  private Map<String,String> localConfMap;

  @Before
  public void setUp() {
    localConfMap = testConfigMap();
  }

  @Test
    public void testCreateDefaultObject() {
      Path absPath = FileSystems.getDefault()
          .getPath(schemaDir)
          .normalize()
          .toAbsolutePath();
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
        Paths.get(absPath.toString(), schemaFile).toUri().toString()
      );
      COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
      WritableObjectFactory<COSObject> f = new COSWritableObjectFactory(cfg);
      assertEquals("Default not COSObject", COSObject.class, f.create().getClass());
    }

  @Test
    public void testCreateJsonObject() {
      Path absPath = FileSystems.getDefault()
          .getPath(schemaDir)
          .normalize()
          .toAbsolutePath();
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
        Paths.get(absPath.toString(), schemaFile).toUri().toString()
      );
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_FORMAT, 
        COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_FORMAT_JSON 
      );
      COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
      WritableObjectFactory<COSObject> f = new COSWritableObjectFactory(cfg);
      assertEquals("Not COSObject", COSObject.class, f.create().getClass());
    }

  @Test
    public void testCreateParquetObject() {
      Path absPath = FileSystems.getDefault()
          .getPath(schemaDir)
          .normalize()
          .toAbsolutePath();
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI, 
        Paths.get(absPath.toString(), schemaFile).toUri().toString()
      );
      localConfMap.put(
        COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_FORMAT, 
        COSSinkConnectorConfig.CONFIG_VALUE_COS_WRITER_FORMAT_PARQUET 
      );
      COSSinkConnectorConfig cfg = new COSSinkConnectorConfig(localConfMap);
      WritableObjectFactory<COSObject> f = new COSWritableObjectFactory(cfg);
      WritableObject o = f.create();
      assertEquals("Not COSParquetObject", COSParquetObject.class, o.getClass());
      assertNotEquals("Is COSObject", COSObject.class, o.getClass());
    }

}
