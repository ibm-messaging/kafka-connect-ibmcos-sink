package com.ibm.cos.endpoints;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    public static Endpoints fetch(String endpointsURL) throws IOException {
        final URL url = new URL(endpointsURL);
        final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setInstanceFollowRedirects(true);
        connection.setReadTimeout(15 * 1000);

        final int responseCode = connection.getResponseCode();
        if (responseCode < 200 || responseCode > 299) {
            throw new IOException("Unable to retrieve endpoint information from: " + endpointsURL);
        }

        final JsonObject root = JsonObject.readFrom(new InputStreamReader(connection.getInputStream()));

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
