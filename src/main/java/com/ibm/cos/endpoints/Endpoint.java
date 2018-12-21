package com.ibm.cos.endpoints;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public class Endpoint {

    private final String publicEndpoint;
    private final String privateEndpoint;

    private Endpoint(final String publicEndpoint, final String privateEndpoint) {
        this.publicEndpoint = publicEndpoint;
        this.privateEndpoint = privateEndpoint;
    }

    public String publicEndpoint() {
        return publicEndpoint;
    }

    public String privateEndpoint() {
        return privateEndpoint;
    }

    static Endpoint parse(final String name, final JsonObject json) {
        final String publicEndpoint = parseValue(name, json.get("public").asObject());
        final String privateEndpoint = parseValue(name, json.get("private").asObject());
        if (publicEndpoint == null || privateEndpoint == null) {
            return null;
        }
        return new Endpoint(publicEndpoint, privateEndpoint);
    }

    private static String parseValue(String name, JsonObject json) {
        JsonValue value = json.get(name);
        if (value == null) {
            value = json.get(name + "-geo");
        }
        return value == null ? null : value.asString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((privateEndpoint == null) ? 0 : privateEndpoint.hashCode());
        result = prime * result + ((publicEndpoint == null) ? 0 : publicEndpoint.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Endpoint other = (Endpoint) obj;
        if (privateEndpoint == null) {
            if (other.privateEndpoint != null)
                return false;
        } else if (!privateEndpoint.equals(other.privateEndpoint))
            return false;
        if (publicEndpoint == null) {
            if (other.publicEndpoint != null)
                return false;
        } else if (!publicEndpoint.equals(other.publicEndpoint))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "public=" + publicEndpoint + ", private=" + privateEndpoint;
    }
}
