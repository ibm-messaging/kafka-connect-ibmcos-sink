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

import java.net.URI;
import java.net.URISyntaxException;

import com.ibm.cloud.objectstorage.services.kms.model.UnsupportedOperationException;
import com.ibm.eventstreams.connect.cossink.COSSinkConnectorConfig;

import org.apache.kafka.common.config.AbstractConfig;

/**
 * Creates instances of the {@link com.ibm.eventstreams.connect.cossink.partitionwriter.SchemaProvider}
 * implementations based on the connector configuration.
 */
public class SchemaProviderFactory {

  private final String uriString;

  /**
   * Constructor.
   * @param config connector configuration.
   */
  public SchemaProviderFactory(AbstractConfig config) {
    String s = config.getString(COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI);
    if (s == null) {
      throw new IllegalArgumentException(
        String.format("%s is undefined",COSSinkConnectorConfig.CONFIG_NAME_COS_WRITER_SCHEMA_URI)
      );
    }
    this.uriString = s;
  }

  public SchemaProvider create() {
    URI uri;
    try {
      uri = new URI(this.uriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Cannot parse URI %s", uriString));
    }
    if ("file".equals(uri.getScheme()) || uri.getScheme() == null) {
      return new FileSchemaProvider(uri);
    }
    else {
      throw new UnsupportedOperationException(String.format("URI schema for %s not implemented", uriString));
    }
  }

}
