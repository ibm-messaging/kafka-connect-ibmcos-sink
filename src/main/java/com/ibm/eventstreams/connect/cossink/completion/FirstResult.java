package com.ibm.eventstreams.connect.cossink.completion;

/**
 * The result from testing whether adding the first {code SinkRecord}
 * to an object will complete the object.
 */
public enum FirstResult {

    /**
     * Adding the first {@code SinkRecord} to the object completed it.
     */
    COMPLETE,

    /**
     * The object is not complete.
     */
    INCOMPLETE

}
