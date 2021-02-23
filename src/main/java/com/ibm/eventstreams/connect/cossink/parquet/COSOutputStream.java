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

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

/**
 * This class interacts directly with COS API to write Parquet output.
 * It is the parent class of COSParquetOutputStream and is instantiated
 * when COSParquetOutputStream is instantiated
 *
 * @author Bill Li
 */
public class COSOutputStream extends PositionOutputStream {

    private long position;
    private ByteBuffer byteBuffer;
    private Bucket bucket;
    private String key;

    /**
     * Prepare values for writing Parquet to COS
     * @param bucket COS bucket object
     * @param filename File name of Parquet file
     * @param parquetBufferSize Allocated Parquet buffer size
     */
    public COSOutputStream(final Bucket bucket, final String filename, final int parquetBufferSize) {
        this.position = 0L;
        this.byteBuffer = ByteBuffer.allocate(parquetBufferSize);
        this.bucket = bucket;
        this.key = filename;
    }

    @Override
    public long getPos() {
        return position;
    }

    /**
     * Place one byte at a time into Byte Buffer and increment position.
     * This method involves converting integer type to byte type. We have
     * verified that there is no issue in accepting Unicode character.
     * This method is inherited from parent class OutputStream. For detailed
     * information, please refer to the original source Java Doc.
     * @param b: Parquet bytes
     */
    @Override
    public void write(int b) {
        byteBuffer.put((byte) b);
        position++;
    }

    /**
     * Ready to commit to COS by calling uploadToCOS method
     */
    public void commit() {
        uploadToCOS(getBufferBytes());
    }

    /**
     * Upload Parquet byte array to COS
     * @param value: Byte array to be committed to COS
     */
    private void uploadToCOS(byte[] value) {
        bucket.putObject(key, new ByteArrayInputStream(value), createMetadata(value));
        byteBuffer.clear();
    }

    /**
     * Turn valid byte content from Byte Buffer into byte array
     * Note that we need to do rewind operation in order to retrieve
     * bytes from the beginning to the end of valid bytes
     * @return return Parquet byte array
     */
    private byte[] getBufferBytes() {
        byte[] value = new byte[byteBuffer.position()];
        byteBuffer.rewind();
        byteBuffer.get(value);
        return value;
    }

    /**
     * Create COS object metadata
     * @param value Byte array containing Parquet output bytes
     * @return Object metadata to commit record to COS
     */
    private static ObjectMetadata createMetadata(final byte[] value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(value.length);
        return metadata;
    }
}
