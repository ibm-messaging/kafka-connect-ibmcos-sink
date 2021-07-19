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

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema provider that loads the Avro schema from a file URI.
 * 
 * If the URI is a directory, all files in that directory are ordered lexicographically
 * and parsed, and the last parsed schema is returned. This allows for complex
 * schemas that referenc other schemas parsed previously. The user must ensure
 * the schema files are named in such a way that the correct processing order
 * is maintained.
 */
public class FileSchemaProvider implements SchemaProvider {

  private static final Logger LOG = LoggerFactory.getLogger(FileSchemaProvider.class);
  private Schema schema = null;
  private final URI uri;
  public FileSchemaProvider(URI uri) {
    this.uri = uri;
  }

  @Override
  public Schema getSchema() {
    if (this.schema == null) {
      try {
        Path path = Paths.get(this.uri);
        if (Files.isDirectory(path)) {
          LOG.debug("Load from directory {}", path);
          this.schema = loadFromDirectory(path);
        }
        else if (Files.isRegularFile(path)) {
          LOG.debug("Load from file {}", path);
          this.schema = loadFromFile(path);
        } 
        else {
          throw new ConnectException("Unknown file type "+this.uri);
        }
        if (this.schema != null) {
          LOG.debug("Final schema {}", this.schema.getFullName());
        }
        else {
          LOG.error("No valid schema available in {}", path);
        }
      } catch (IOException e) {
        throw new ConnectException("Unable to parse schema from "+this.uri, e);
      }
    }
    return this.schema;
  }

  private Schema loadFromDirectory(Path path) throws IOException {
    Schema sch = null;
    Parser parser = new Schema.Parser();
    // sort file names in this directory and parse them in that sequence
    try (DirectoryStream<Path> ds = Files.newDirectoryStream(path)) {
      List<Path> fileList = new ArrayList<>();
      ds.forEach(fileList::add);
      fileList.removeIf(p -> !Files.isRegularFile(p));
      fileList.sort(Comparator.comparing(x -> x.getFileName().toString()));
      for (Path p: fileList) {
        sch  = parser.parse(p.toFile());
      }
    }
    return sch;
  }

  private Schema loadFromFile(Path path) throws IOException {
    Parser parser = new Schema.Parser();
    return parser.parse(path.toFile());
  }
}
