/*
 * Copyright 2019 IBM Corporation
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

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * PartitionWriter instances writes {@code SinkRecords} for one Kafka partition
 * to IBM COS. An instance is created at the point a partition is assigned to
 * a SinkTask and is closed when the partition is no longer assigned to the task.
 */
public interface PartitionWriter {

    /**
     * preCommit is called just before the connector commits offsets for this partition.
     * @return the offset to commit for this partition, or <code>null</code> if the offset
     *       hasn't changed since the last time {@code #preCommit()} was called.
     */
    Long preCommit();

    /**
     * put passes a {@code SinkRecord} to this writer for processing.
     * @param record
     */
    void put(SinkRecord record);

    /**
     * close is called when the writer is no longer required.
     */
    void close();
}
