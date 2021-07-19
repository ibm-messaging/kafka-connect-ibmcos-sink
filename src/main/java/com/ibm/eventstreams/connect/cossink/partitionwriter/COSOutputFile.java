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

import java.io.IOException;
import com.ibm.cos.Bucket;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class COSOutputFile implements OutputFile {
  private int bufferSize;
  private String filename;
  private Bucket bucket;

  /**
   * Constructor.
   * @param bucket target COS bucket
   * @param filename name of the COS object
   * @param bufferSize buffer size
   */
  public COSOutputFile(Bucket bucket, String filename, int bufferSize) {
    this.bucket = bucket;
    this.filename = filename;
    this.bufferSize = bufferSize;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new COSOutputStream(this.bucket, this.filename, this.bufferSize);
}

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return create(blockSizeHint);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() { // not applicable
    return 0;
  }
}
