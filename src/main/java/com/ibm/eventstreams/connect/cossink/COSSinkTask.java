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
package com.ibm.eventstreams.connect.cossink;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.cos.Bucket;
import com.ibm.cos.Client;
import com.ibm.cos.ClientFactory;
import com.ibm.cos.ClientFactoryImpl;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;
import com.ibm.eventstreams.connect.cossink.completion.DeadlineCriteria;
import com.ibm.eventstreams.connect.cossink.completion.RecordCountCriteria;
import com.ibm.eventstreams.connect.cossink.completion.RecordIntervalCriteria;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineServiceImpl;
import com.ibm.eventstreams.connect.cossink.partitionwriter.COSPartitionWriterFactory;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriter;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriterFactory;

public class COSSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(COSSinkTask.class);

    private final ClientFactory clientFactory;
    private final PartitionWriterFactory pwFactory;
    private final Map<TopicPartition, PartitionWriter> assignedWriters;
    private final DeadlineService deadlineService;

    private Bucket bucket;
    private int recordsPerObject;
    private int deadlineSec;
    private int intervalSec;

    // Connect framework requires no-value constructor.
    public COSSinkTask() {
        this(new ClientFactoryImpl(), new COSPartitionWriterFactory(), new HashMap<>(), new DeadlineServiceImpl());
    }

    // For unit test, allows for dependency injection.
    COSSinkTask(
            ClientFactory clientFactory, PartitionWriterFactory pwFactory,
            Map<TopicPartition, PartitionWriter> assignedWriters,
            DeadlineService deadlineService) {
        this.clientFactory = clientFactory;
        this.pwFactory = pwFactory;
        this.assignedWriters = assignedWriters;
        this.deadlineService = deadlineService;
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return COSSinkConnector.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        COSSinkConnectorConfig connectorConfig = new COSSinkConnectorConfig(props);
        final Password apiKey = connectorConfig.getPassword(COSSinkConnectorConfig.CONFIG_NAME_OS_API_KEY);
        final String bucketLocation = connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_LOCATION);
        final String bucketName = connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_NAME);
        final String bucketResiliency = connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_RESILIENCY);
        final String endpointType = connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_OS_ENDPOINT_VISIBILITY);
        final String serviceCRN = connectorConfig.getString(COSSinkConnectorConfig.CONFIG_NAME_OS_SERVICE_CRN);

        final Client client = clientFactory.newClient(apiKey.value(), serviceCRN, bucketLocation, bucketResiliency, endpointType);
        bucket = client.bucket(bucketName);

        recordsPerObject = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS);
        deadlineSec = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS);
        intervalSec = connectorConfig.getInt(COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS);

        if (recordsPerObject < 1 && deadlineSec < 1 && intervalSec < 1) {
            throw new ConfigException(
                    "At least one of: '" + COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS + "', " +
                            COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS + "', or '" +
                            COSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS + "' must be set " +
                            "to a value that is greater than zero");
        }

        open(context.assignment());
        LOG.info("Starting");
    }

    private CompletionCriteriaSet buildCompletionCriteriaSet() {
        final CompletionCriteriaSet completionCriteria = new CompletionCriteriaSet();
        if (recordsPerObject > 0) {
            completionCriteria.add(new RecordCountCriteria(recordsPerObject));
        }
        if (deadlineSec > 0) {
            completionCriteria.add(new DeadlineCriteria(deadlineService, deadlineSec));
        }
        if (intervalSec > 0) {
            completionCriteria.add(new RecordIntervalCriteria(intervalSec));
        }
        return completionCriteria;
    }

    /**
     * The SinkTask use this method to create writers for newly assigned partitions in case of partition
     * rebalance. This method will be called after partition re-assignment completes and before the SinkTask starts
     * fetching data. Note that any errors raised from this method will cause the task to stop.
     * @param partitions The list of partitions that are now assigned to the task (may include
     *                 partitions previously assigned to the task)
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            if (assignedWriters.containsKey(tp)) {
                LOG.info("A PartitionWriter already exists for {}", tp);
            } else {
                PartitionWriter pw = pwFactory.newPartitionWriter(bucket, buildCompletionCriteriaSet());
                assignedWriters.put(tp, pw);
            }
        }
    }

    /**
     * The SinkTask use this method to close writers for partitions that are no
     * longer assigned to the SinkTask. This method will be called before a rebalance operation starts
     * and after the SinkTask stops fetching data. After being closed, Connect will not write
     * any records to the task until a new set of partitions has been opened. Note that any errors raised
     * from this method will cause the task to stop.
     * @param partitions The list of partitions that should be closed
     */
    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            final PartitionWriter pw = assignedWriters.remove(tp);
            if (pw == null) {
                LOG.info("No PartitionWriter exist for {}", tp);
            } else {
                pw.close();
            }
        }
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        LOG.info("Stopping");
        bucket = null;
        deadlineService.close();
        assignedWriters.clear();
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            final TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            final PartitionWriter pw = assignedWriters.get(tp);
            if (pw == null) {
                LOG.error("Unable to find PartitionWriter for {}", tp);
            } else {
                assignedWriters.get(tp).put(record);
            }
        }
    }

    /**
     * Pre-commit hook invoked prior to an offset commit.
     *
     * The default implementation simply invokes {@link #flush(Map)} and is thus able to assume all {@code currentOffsets} are safe to commit.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                     provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                     passed to {@link #put}.
     *
     * @return an empty map if Connect-managed offset commit is not desired, otherwise a map of offsets by topic-partition that are safe to commit.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
      final Map<TopicPartition, OffsetAndMetadata> result = new HashMap<>();
      for (Map.Entry<TopicPartition, PartitionWriter> entry : assignedWriters.entrySet()) {
          final Long offset = entry.getValue().preCommit();
          if (offset != null) {
              result.put(entry.getKey(), new OffsetAndMetadata(offset));
          }
      }
      return result;
    }

}
