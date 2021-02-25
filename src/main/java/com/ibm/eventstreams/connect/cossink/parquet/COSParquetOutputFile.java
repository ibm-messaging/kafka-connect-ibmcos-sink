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
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * This class is an implementation of interface OutputFile mainly used for
 * instantiating class AvroParquetWriter.
 * It is instantiated inside class ParquetRecordWriterProvider.
 *
 * @author Bill Li
 */
public class COSParquetOutputFile implements OutputFile {

    private Bucket bucket;
    private String filename;
    private int parquetBufferSize;
    private COSParquetOutputStream cosOut;

    /**
     * Setting values for creating COSParquetOutputStream
     * @param bucket COS bucket object
     * @param filename File name of Parquet file
     * @param parquetBufferSize Allocated Parquet buffer size
     */
    public COSParquetOutputFile(Bucket bucket, String filename, int parquetBufferSize) {
        this.bucket = bucket;
        this.filename = filename;
        this.parquetBufferSize = parquetBufferSize;
    }

    /**
     * Getter method for COSParquetOutputStream object
     * @return COSParquetOutputStream object
     */
    public COSParquetOutputStream getCosOut() {
        return cosOut;
    }

    /**
     * Create COSParquetOutputStream instance for writing Parquet output with
     * COS bucket object, file name, and Parquet buffer size
     * @param blockSizeHint Block size hint is not used
     * @return Position output stream
     */
    @Override
    public PositionOutputStream create(long blockSizeHint) {
        cosOut = new COSParquetOutputStream(bucket, filename, parquetBufferSize);
        return cosOut;
    }

    /**
     * Calls create method to create COSParquetOutputStream
     * @param blockSizeHint Block size hint is not used
     * @return PositionOutputStream
     */
    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    /**
     * Not used
     * @return true or false
     */
    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    /**
     * Not used
     * @return block size of type long
     */
    @Override
    public long defaultBlockSize() {
        return 0;
    }
}
