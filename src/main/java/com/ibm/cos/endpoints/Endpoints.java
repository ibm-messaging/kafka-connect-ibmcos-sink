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

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;

public class Endpoints {

    private final String iamToken;
    private final String iamPolicy;
    private final Map<String, Endpoint> crossRegion;
    private final Map<String, Endpoint> regional;
    private final Map<String, Endpoint> singleSite;

    private Endpoints(final String iamPolicy,
                      final String iamToken,
                      final Map<String, Endpoint> crossRegion,
                      final Map<String, Endpoint> regional,
                      final Map<String, Endpoint> singleSite) {
        this.iamPolicy = iamPolicy;
        this.iamToken = iamToken;
        this.crossRegion = Collections.unmodifiableMap(crossRegion);
        this.regional = Collections.unmodifiableMap(regional);
        this.singleSite = Collections.unmodifiableMap(singleSite);
    }

    public String iamPolicy() {
        return iamPolicy;
    }

    public String iamToken() {
        return iamToken;
    }

    public Map<String, Endpoint> crossRegion() {
        return crossRegion;
    }

    public Map<String, Endpoint> regional() {
        return regional;
    }

    public Map<String, Endpoint> singleSite() {
        return singleSite;
    }

    public static void main(String[] args) throws Exception {
        Endpoints ep = fetch(args[0]);
        System.out.println(ep.crossRegion());
    }

    public static Endpoints fetch(String endpointsURL) throws IOException {
        final URL url = new URL(endpointsURL);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setInstanceFollowRedirects(true);
        connection.setRequestProperty("User-Agent", "kafka-connect-ibmcos-sink");
        connection.setReadTimeout(15 * 1000);

        final int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode > 299) {
            throw new IOException("Unable to retrieve endpoint information from: " + endpointsURL + ", got code: " + responseCode);
        }

        final JsonObject root = Json.parse(new InputStreamReader(connection.getInputStream())).asObject();

        final JsonObject identityEndpoints = root.get("identity-endpoints").asObject();
        final String iamToken = identityEndpoints.get("iam-token").asString();
        final String iamPolicy = identityEndpoints.get("iam-policy").asString();

        final JsonObject serviceEndpoints = root.get("service-endpoints").asObject();
        final Map<String, Endpoint> crossRegion = parse(serviceEndpoints.get("cross-region").asObject());
        final Map<String, Endpoint> singleSite = parse(serviceEndpoints.get("single-site").asObject());
        final Map<String, Endpoint> regional = parse(serviceEndpoints.get("regional").asObject());

        return new Endpoints(iamPolicy, iamToken, crossRegion, regional, singleSite);
    }

    private static Map<String, Endpoint> parse(final JsonObject parent) {
        Map<String, Endpoint> result = new HashMap<>();
        for (JsonObject.Member property : parent) {
            final String key = property.getName();
            final Endpoint value = Endpoint.parse(key, parent.get(key).asObject());
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }
}
