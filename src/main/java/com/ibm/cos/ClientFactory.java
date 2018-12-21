package com.ibm.cos;

import com.ibm.cloud.objectstorage.services.s3.AmazonS3;

public interface ClientFactory {

    AmazonS3 newClient(String apiKey, String serviceCRN, String bucketLocation, String bucketResiliency, String endpointType);

}
