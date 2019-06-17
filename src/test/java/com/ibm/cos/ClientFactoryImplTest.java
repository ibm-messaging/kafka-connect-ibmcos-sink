/*
 * Copyright 2019 IBM Corporation
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
package com.ibm.cos;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

public class ClientFactoryImplTest {
    
    private MockWebServer server;
    private String url;

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        url = server.url("/").toString();
        String json = new String(Files.readAllBytes(Paths.get("./src/test/resources/endpoints.json")));
        server.enqueue(new MockResponse().setResponseCode(500));
        server.enqueue(new MockResponse().setBody(json));
    }

    @After
    public void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    public void testConstructor() throws IOException {
        try {
            new ClientFactoryImpl(url);
            fail("Should have thrown ClientFactoryException");
        } catch (ClientFactoryException cfe) {
            //expected
        }
        
        ClientFactoryImpl clientfactory = new ClientFactoryImpl(url);
        clientfactory.newClient("apiKey", "serviceCRN", "eu", "cross-region", "public");
        clientfactory.newClient("apiKey", "serviceCRN", "eu", "cross-region", "private");
        clientfactory.newClient("apiKey", "serviceCRN", "eu-de", "regional", "public");
        clientfactory.newClient("apiKey", "serviceCRN", "ams03", "single-site", "public");
        try {
            clientfactory.newClient("apiKey", "serviceCRN", "eu", "something", "public");
            fail("Should have thrown ClientFactoryException");
        } catch (ClientFactoryException cfe) {
            //expected
        }
        try {
            clientfactory.newClient("apiKey", "serviceCRN", "ams03", "single-site", "blah");
            fail("Should have thrown ClientFactoryException");
        } catch (ClientFactoryException cfe) {
            //expected
        }
        try {
            clientfactory.newClient("apiKey", "serviceCRN", "blah", "single-site", "public");
            fail("Should have thrown ClientFactoryException");
        } catch (ClientFactoryException cfe) {
            //expected
        }
    }

}
