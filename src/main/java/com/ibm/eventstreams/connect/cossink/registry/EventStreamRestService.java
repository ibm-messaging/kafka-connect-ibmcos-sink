package com.ibm.eventstreams.connect.cossink.registry;

import org.apache.avro.Schema;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

public class EventStreamRestService {

    private static final Logger LOG = LoggerFactory.getLogger(EventStreamRestService.class);

    private String baseUrl;
    private String apiKey;
    private Schema.Parser schemaParser;

    public EventStreamRestService(String baseUrl, String apiKey) {

        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.schemaParser = new Schema.Parser();
    }

    public Schema getSchemaBySubject(String subject) throws IOException {

        String schemaPath = getSchemaPath(subject);
        return this.httpRequest(schemaPath);
    }

    private String getSchemaPath(String subject) {

        return String.format("%s/artifacts/%s", baseUrl, subject);
    }

    private Schema httpRequest(String schemaPath) throws IOException {

        HttpClient httpClient = HttpClientBuilder.create().build();

        Base64.Encoder encoder = Base64.getEncoder();
        String encoderString = String.format("token:%s", apiKey);
        String encoding = encoder.encodeToString(encoderString.getBytes());

        HttpGet httpGet = new HttpGet(schemaPath);
        httpGet.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoding);

        return this.sendRequest(httpClient, httpGet);
    }

    private Schema sendRequest(HttpClient httpClient, HttpGet httpGet) throws IOException {

        HttpResponse response;

        try {
            response = httpClient.execute(httpGet);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to get response from client", e);
        }

        InputStream schemaStream = response.getEntity().getContent();
        Schema avroSchema = toAvroSchema(schemaStream);
        schemaStream.close();

        return avroSchema;
    }

    private Schema toAvroSchema(InputStream schemaStream) throws IOException {

        return schemaParser.parse(schemaStream);
    }
}
