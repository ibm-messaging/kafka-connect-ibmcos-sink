package com.ibm.eventstreams.connect.cossink.registry;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class SchemaFileClient {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaFileClient.class);

    private String filePath;
    private Schema.Parser schemaParser;

    public SchemaFileClient(String filePath) {

        this.filePath = filePath;
        this.schemaParser = new Schema.Parser();
    }

    public Schema getByFile() {

        Schema avroSchema;

        try {
            File avroSchemaFile = new File(filePath);
            avroSchema = schemaParser.parse(avroSchemaFile);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to parse Avro schema", e);
        }

        return avroSchema;
    }
}
