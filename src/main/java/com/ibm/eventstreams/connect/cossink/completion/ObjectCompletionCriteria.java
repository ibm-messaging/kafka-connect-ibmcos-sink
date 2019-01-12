package com.ibm.eventstreams.connect.cossink.completion;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for classes that determine which SinkRecords make up an object.
 */
public interface ObjectCompletionCriteria {

    /**
     * Called with the first {@code SinkRecord} that makes up a new object storage
     * object.
     *
     * @param sinkRecord the next Kafka record from the partition.
     *
     * @param asyncCompleter an object that allows another thread to indicate the object is
     *             complete *outside* the scope of both the
     *             {@code ObjectCompletionCriteria#first(SinkRecord, AsyncCompleter)}
     *             and {@code ObjectCompletionCriteria#next(SinkRecord)} methods. For example,
     *             this can be used to implement completion based on wall-clock time, where
     *             the decision to complete the object is determined by logic not contained
     *             in the {@code ObjectCompletionCriteria#next(SinkRecord} method.
     *
     * @return a {@code FirstResult} that indicates whether this {@code SinkRecord} completes
     *             the current object.
     */
    FirstResult first(SinkRecord sinkRecord, AsyncCompleter asyncCompleter);

    /**
     * Next is called repeatedly for each subsequent {@code SinkRecord} that may make
     * up the object.
     *
     * @param sinkRecord the next Kafka record from the partition.
     *
     * @return a {@code NextResult} that indicates whether this {@code SinkRecord} is part
     *             of the current object, completes the current object, or should be
     *             part of a new object.
     */
    NextResult next(SinkRecord sinkRecord);

    /**
     * Finally complete is called to notify the implementation that the object has been
     * completed. This may be as a result of the actions taken by this implementation of
     * {@code ObjectCompletionCriteria}, or it may be due to the actions of another instance.
     */
    void complete();

}
