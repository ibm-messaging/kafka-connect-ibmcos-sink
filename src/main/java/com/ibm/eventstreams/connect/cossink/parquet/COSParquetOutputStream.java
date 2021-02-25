/*
 * IBM Confidential Source Materials
 * (C) Copyright IBM Corp. 2020
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 *
 */
package com.ibm.eventstreams.connect.cossink.parquet;

import com.ibm.cos.Bucket;

import java.io.IOException;

/**
 * This class is responsible for passing values to COSOutputStream.
 * It is instantiated inside class COSParquetOutputFile.
 *
 * @author Bill Li
 */
public class COSParquetOutputStream extends COSOutputStream {

    private volatile boolean commit;

    /**
     * Instantiate parent class COSOutputStream
     * @param bucket COS bucket object
     * @param filename File name of Parquet file
     * @param parquetBufferSize Allocated Parquet buffer size
     */
    public COSParquetOutputStream(Bucket bucket, String filename, int parquetBufferSize) {
        super(bucket, filename, parquetBufferSize);
    }

    @Override
    public void close() throws IOException {
        if (commit) {
            super.commit();
            commit = false;
        } else {
            super.close();
        }
    }

    /**
     * Setter method for commit
     */
    public void setCommit() {
        commit = true;
    }
}
