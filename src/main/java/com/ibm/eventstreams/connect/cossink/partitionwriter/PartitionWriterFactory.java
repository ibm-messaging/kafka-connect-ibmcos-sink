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

import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;
import com.ibm.eventstreams.connect.cossink.parquet.COSParquetConfig;

public interface PartitionWriterFactory {

    /**
     * Creates a new {@code PartitionWriter} instance
     * @param bucket the COS bucket that the writer will write into.
     * @param a set of criteria used to determine when sufficient Kafka records
     *           have been read and can be written as an object storage object.
     * @return
     */
    PartitionWriter newPartitionWriter(final Bucket bucket,
                                       final CompletionCriteriaSet completionCriteira,
                                       final Boolean recordDelimiter,
                                       final COSParquetConfig cosParquetConfig);
}
