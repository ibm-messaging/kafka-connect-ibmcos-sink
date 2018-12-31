package com.ibm.eventstreams.connect.cossink;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import com.ibm.cos.Bucket;
import com.ibm.cos.Client;
import com.ibm.cos.ClientFactory;
import com.ibm.cos.ClientFactoryImpl;
import com.ibm.eventstreams.connect.cossink.partitionwriter.OSPartitionWriterFactory;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriter;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriterFactory;

public class OSSinkTask extends SinkTask {

    private final ClientFactory clientFactory;
    private final PartitionWriterFactory pwFactory;
    private Bucket bucket;
    private int recordsPerObject;
    private int deadlineSec;
    private Map<TopicPartition, PartitionWriter> assignedWriters;

    // Connect framework requires no-value constructor.
    public OSSinkTask() throws IOException {
        this(new ClientFactoryImpl(), new OSPartitionWriterFactory(), new HashMap<>());
    }

    // For unit test, allows for dependency injection.
    OSSinkTask(
            ClientFactory clientFactory, PartitionWriterFactory pwFactory,
            Map<TopicPartition, PartitionWriter> assignedWriters) {
        this.clientFactory = clientFactory;
        this.pwFactory = pwFactory;
        this.assignedWriters = assignedWriters;
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return OSSinkConnector.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {

        final String apiKey = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_API_KEY);
        final String bucketLocation = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_LOCATION);
        final String bucketName = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_NAME);
        final String bucketResiliency = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_RESILIENCY);
        final String endpointType = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_ENDPOINT_VISIBILITY);
        final String serviceCRN = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_SERVICE_CRN);

        final Client client = clientFactory.newClient(apiKey, serviceCRN, bucketLocation, bucketResiliency, endpointType);
        bucket = client.bucket(bucketName);

        recordsPerObject = Integer.parseInt(props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS));
        deadlineSec = Integer.parseInt(props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS));

        open(context.assignment());
    }

    /**
     * The SinkTask use this method to create writers for newly assigned partitions in case of partition
     * rebalance. This method will be called after partition re-assignment completes and before the SinkTask starts
     * fetching data. Note that any errors raised from this method will cause the task to stop.
     * @param partitions The list of partitions that are now assigned to the task (may include
     *                   partitions previously assigned to the task)
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            if (assignedWriters.containsKey(tp)) {
                // TODO: log
            } else {
                PartitionWriter pw = pwFactory.newPartitionWriter(deadlineSec, recordsPerObject, bucket);
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
                // TODO: log
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
        bucket = null;
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
                // TODO: log
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
     *                       provided for convenience but could also be determined by tracking all offsets included in the {@link SinkRecord}s
     *                       passed to {@link #put}.
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
