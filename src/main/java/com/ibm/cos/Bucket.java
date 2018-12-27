package com.ibm.cos;

import java.io.InputStream;

import com.ibm.cloud.objectstorage.AmazonServiceException;
import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;

public interface Bucket {

    PutObjectResult putObject(String key, InputStream input, ObjectMetadata metadata)
            throws SdkClientException, AmazonServiceException;
}
