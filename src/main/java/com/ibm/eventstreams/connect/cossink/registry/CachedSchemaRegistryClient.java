package com.ibm.eventstreams.connect.cossink.registry;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CachedSchemaRegistryClient implements SchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(CachedSchemaRegistryClient.class);

    private EventStreamRestService eventStreamRestService;
    private Map<String, Schema> subjectSchemaMap;

    public CachedSchemaRegistryClient(String baseUrl, String apiKey) {

        this.eventStreamRestService = new EventStreamRestService(baseUrl, apiKey);
        this.subjectSchemaMap = new HashMap<>();
    }

    @Override
    public synchronized Schema getSchemaBySubjectFromRegistry(String subject) throws IOException {

        return eventStreamRestService.getSchemaBySubject(subject);
    }

    @Override
    public synchronized Schema getBySubject(String subject) {

        Schema cachedSchema = subjectSchemaMap.get(subject);
        if (cachedSchema != null) {
            return cachedSchema;
        } else {
            Schema retrievedSchema = null;
            try {
                retrievedSchema = this.getSchemaBySubjectFromRegistry(subject);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to parse Avro schema", e);
            }
            subjectSchemaMap.put(subject, retrievedSchema);
            return retrievedSchema;
        }
    }
}
