package com.ibm.eventstreams.connect.cossink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cos.ClientFactory;
import com.ibm.cos.ClientFactoryImpl;

public class OSSinkTask extends SinkTask {

    private static final Charset UTF8 = Charset.forName("UTF8");

    private final ClientFactory clientFactory;
    private AmazonS3 client;
    private String bucketName;

    // Connect framework requires no-value constructor.
    public OSSinkTask() throws IOException {
        this(new ClientFactoryImpl());
    }

    // For unit test, allows injection of clientFactory.
    OSSinkTask(ClientFactory clientFactory) {
        this.clientFactory = clientFactory;
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
        bucketName = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_NAME);

        final String apiKey = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_API_KEY);
        final String serviceCRN = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_SERVICE_CRN);
        final String bucketLocation = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_LOCATION);
        final String bucketResiliency = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_BUCKET_RESILIENCY);
        final String endpointType = props.get(OSSinkConnectorConfig.CONFIG_NAME_OS_ENDPOINT_VISIBILITY);

        client = clientFactory.newClient(apiKey, serviceCRN, bucketLocation, bucketResiliency, endpointType);
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        client = null;
        bucketName = null;
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
            final String key = createKey(record);
            final byte[] value = createValue(record);
            final ObjectMetadata metadata = createMetadata(key, value);

            client.putObject(bucketName, key, new ByteArrayInputStream(value), metadata);
        }
    }

    private static String createKey(final SinkRecord record) {
        return String.format("%4d-%d", record.kafkaPartition(), record.kafkaOffset());
    }

    private static byte[] createValue(final SinkRecord record) {
        final Schema schema = record.valueSchema();
        byte[] result = null;
        if (schema == null || schema.type() == Type.BYTES) {
            if (record.value() instanceof byte[]) {
                result = (byte[])record.value();
            } else if (record.value() instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer)record.value();
                result = new byte[bb.remaining()];
                bb.get(result);
            }
        }

        if (result == null) {
            result = record.value().toString().getBytes(UTF8);
        }
        return result;
    }

    private static ObjectMetadata createMetadata(final String key, final byte[] value) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(value.length);
        return metadata;
    }
}
