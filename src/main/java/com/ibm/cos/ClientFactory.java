package com.ibm.cos;

public interface ClientFactory {

    Client newClient(String apiKey, String serviceCRN, String bucketLocation, String bucketResiliency, String endpointType);

}
