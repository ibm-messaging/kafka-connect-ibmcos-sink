/*
 * Copyright 2019, 2021 IBM Corporation
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

public class EndpointTest {

    @Test
    public void testParse() throws IOException {
        Endpoint usEndpoint = fromFile("us");
        assertEquals("s3.private.us.cloud-object-storage.appdomain.cloud", usEndpoint.privateEndpoint());
        assertEquals("s3.us.cloud-object-storage.appdomain.cloud", usEndpoint.publicEndpoint());
        assertEquals("s3.direct.us.cloud-object-storage.appdomain.cloud", usEndpoint.directEndpoint());
    }

    private Endpoint fromFile(String name) throws IOException {
        String jsonStr = new String(Files.readAllBytes(Paths.get("./src/test/resources/endpoints.json")));
        JsonObject json = Json.parse(jsonStr).asObject();
        JsonObject crossRegion = json.get("service-endpoints").asObject().get("cross-region").asObject();
        JsonObject value = crossRegion.get(name).asObject();
        return Endpoint.parse(name, value);
    }
 
    @Test
    public void testEquals() throws IOException {
        Endpoint usEndpoint = fromFile("us");
        Endpoint usEndpoint2 = fromFile("us");
        Endpoint euEndpoint = fromFile("eu");
        assertEquals(usEndpoint, usEndpoint2);
        assertNotEquals(euEndpoint, usEndpoint);
    }

    @Test
    public void testToString() throws IOException {
        Endpoint usEndpoint = fromFile("us");
        assertTrue(usEndpoint.toString().contains(usEndpoint.publicEndpoint()));
    }
}
