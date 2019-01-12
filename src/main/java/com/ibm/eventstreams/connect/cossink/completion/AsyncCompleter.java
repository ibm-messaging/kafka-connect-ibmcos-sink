package com.ibm.eventstreams.connect.cossink.completion;

/**
 * AsyncCompleter allows for the completion of an object to be triggered
 * from outside of the {@code ObjectCompletionCriteria#test(org.apache.kafka.connect.sink.SinkRecord, AsyncCompleter, long)}
 * method.
 */
public interface AsyncCompleter {

    /**
     * Invoked to indicate that the object is now complete.
     */
    void asyncComplete();

}
