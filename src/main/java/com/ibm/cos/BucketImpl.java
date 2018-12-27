package com.ibm.cos;

import java.io.InputStream;

import com.ibm.cloud.objectstorage.AmazonServiceException;
import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;

public class BucketImpl implements Bucket {

    private final AmazonS3 s3;
    private final String name;

    public BucketImpl(final AmazonS3 s3, final String name) {
        this.s3 = s3;
        this.name = name;
    }

    @Override
    public PutObjectResult putObject(String key, InputStream input, ObjectMetadata metadata)
            throws SdkClientException, AmazonServiceException {
        return s3.putObject(name, key, input, metadata);
    }

}

