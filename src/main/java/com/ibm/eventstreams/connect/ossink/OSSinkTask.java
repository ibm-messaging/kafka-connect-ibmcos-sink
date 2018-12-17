package com.ibm.eventstreams.connect.ossink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class OSSinkTask extends SinkTask {

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
        // TODO: implement!
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
        // TODO: implement!
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        // TODO: implement!
    }

}
