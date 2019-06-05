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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.ibm.cloud.objectstorage.ClientConfiguration;
import com.ibm.cloud.objectstorage.SDKGlobalConfiguration;
import com.ibm.cloud.objectstorage.auth.AWSCredentials;
import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.ibm.cloud.objectstorage.oauth.BasicIBMOAuthCredentials;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cos.endpoints.Endpoint;
import com.ibm.cos.endpoints.Endpoints;

public class ClientFactoryImpl implements ClientFactory {

    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    private static Endpoints endpoints;
    private static IOException initFailedException;

    public ClientFactoryImpl() throws IOException {
        if (initialized.compareAndSet(false, true)) {
            try {
                endpoints = Endpoints.fetch("https://control.cloud-object-storage.cloud.ibm.com/v2/endpoints");
                SDKGlobalConfiguration.IAM_ENDPOINT = "https://" + endpoints.iamToken() + "/oidc/token";
            } catch(IOException e) {
                initFailedException = e;
                throw e;
            }
        }
    }

    @Override
    public Client newClient(String apiKey, String serviceCRN, String bucketLocation, String bucketResiliency, String endpointType)
    {
        if (initFailedException != null) {
            throw new ClientFactoryException("COS client failed to initialize due to: " + initFailedException.getMessage());
        }

        Map<String, Endpoint> resiliencyEndpoints = null;
        switch(bucketResiliency) {
        case "cross-region":
            resiliencyEndpoints = endpoints.crossRegion();
            break;
        case "regional":
            resiliencyEndpoints = endpoints.regional();
            break;
        case "single-site":
            resiliencyEndpoints = endpoints.singleSite();
            break;
        default:
            throw new ClientFactoryException("Invalid bucket resiliency: '" + bucketResiliency +
                    "' must be one of: cross-region, regional, or single-site");
        }

        Endpoint endpoint = resiliencyEndpoints.get(bucketLocation);
        if (endpoint == null) {
            throw new ClientFactoryException("Invalid bucket location: '" + bucketLocation +
                    "' must be one of: " + resiliencyEndpoints.keySet());
        }

        String endpointURL = null;
        switch(endpointType) {
        case "public":
            endpointURL = endpoint.publicEndpoint();
            break;
        case "private":
            endpointURL = endpoint.privateEndpoint();
            break;
        default:
            throw new ClientFactoryException("Invalid endpoint visibility: '" + endpointType +
                    "' must be one of: public, or private");
        }

        final AWSCredentials credentials = new BasicIBMOAuthCredentials(apiKey, serviceCRN);
        ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
        clientConfig.setUseTcpKeepAlive(true);

        final AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpointURL, bucketLocation))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfig)
                .build();

        return new ClientImpl(s3);
    }
}
