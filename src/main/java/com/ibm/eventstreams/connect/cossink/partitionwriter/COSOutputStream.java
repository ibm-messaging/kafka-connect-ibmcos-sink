/*
 * Copyright 2021 IBM Corporation
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
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.Bucket;

import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mimics the output stream used by {@link io.confluent.connect.storage.format.RecordWriter},
 * writes a COS object on {@code close()}. {@code flush()} is a no-op.
 */
public class COSOutputStream extends PositionOutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(COSOutputStream.class);

  private ByteBuffer byteBuffer;
  private Bucket bucket;
  private String key;

  /**
   * Constructor
   * @param bucket target COS bucket
   * @param filename name (key) of COS object
   * @param bufferSize stream buffer size; should be able to accommodate the entire 
   *        object being written.
   */
  public COSOutputStream(final Bucket bucket, final String filename, final int bufferSize) {
    this.byteBuffer = ByteBuffer.allocate(bufferSize);
    this.bucket = bucket;
    this.key = filename;
  }

  @Override
  public long getPos() throws IOException {
    return this.byteBuffer.position();
  }

  @Override
  public void write(int b) throws IOException {
    byteBuffer.put((byte) b);
  }

  @Override
  public void flush() {
    LOG.debug("Someone called flush on us");
    // do nothing; we can't do partial writes to COS
  }

  @Override
  public void close() {
    int dataLen = byteBuffer.position(); 
    LOG.trace("> close, writing {} bytes", dataLen);
    byte[] arr;
    int arrOffset = 0;
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(dataLen);
    if (byteBuffer.hasArray()) {
      // avoid copying
      arr = byteBuffer.array();
      arrOffset = byteBuffer.arrayOffset();
    }
    else {
      LOG.debug("No backing array, will copy {} bytes", dataLen);
      arr = new byte[dataLen];
      byteBuffer.rewind();
      byteBuffer.get(arr);  
    }
    bucket.putObject(key, new ByteArrayInputStream(arr, arrOffset, dataLen), metadata);
    LOG.trace("< close");
  }
}
