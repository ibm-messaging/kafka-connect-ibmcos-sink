package com.ibm.eventstreams.connect.cossink.registry;

import org.apache.avro.Schema;

import java.io.IOException;

public interface SchemaRegistryClient {

    Schema getSchemaBySubjectFromRegistry(String subject) throws IOException;

    Schema getBySubject(String subject);
}
