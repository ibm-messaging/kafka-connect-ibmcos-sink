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
package com.ibm.cos.endpoints;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

public class EndpointsTest {

    private MockWebServer server;
    private String url;

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        url = server.url("/").toString();
    }

    @After
    public void tearDown() throws IOException {
        server.shutdown();
    }

    @Test
    public void testFetch() throws IOException {
        String json = new String(Files.readAllBytes(Paths.get("./src/test/resources/endpoints.json")));
        server.enqueue(new MockResponse().setBody(json));
        Endpoints endpoints = Endpoints.fetch(url);
        assertEquals("iampap.cloud.ibm.com", endpoints.iamPolicy());
        assertEquals("iam.cloud.ibm.com", endpoints.iamToken());
        assertEquals(3, endpoints.crossRegion().size());
        assertEquals(6, endpoints.regional().size());
        assertEquals(12, endpoints.singleSite().size());
    }

    @Test(expected=IOException.class)
    public void testFetchFail() throws IOException {
        server.enqueue(new MockResponse().setResponseCode(500));
        Endpoints.fetch(url);
    }
}
