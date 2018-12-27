package com.ibm.cos;

import com.ibm.cloud.objectstorage.services.s3.AmazonS3;

public class ClientImpl implements Client {

    private final AmazonS3 s3;

    ClientImpl(final AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public Bucket bucket(final String name) {
        return new BucketImpl(s3, name);
    }


}
