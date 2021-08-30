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

import com.ibm.cos.Bucket;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Abstraction of an object that can write itself to an object storage service.
 * 
 * It accumulates Kafka Connect {@code SinkRecord}s and writes itself, using 
 * some file format, to a COS bucket.
 */
public interface WritableObject {

  /**
   * Add a {@code SinkRecord} to this object.
   * @param record a {@code SinkRecord} to add.
   */
  public void put(SinkRecord record);

  /**
   * Write accumulated {@code SinkRecord}s, as a single object, to a bucket.
   * @param bucket destination bucket for this object.
   */
  public void write(final Bucket bucket);

  /**
   * Returns offset of the latest SinkRecord.
   * @return Kafka topic offset of the latest record put in this object.
   */
  public Long lastOffset();
}
