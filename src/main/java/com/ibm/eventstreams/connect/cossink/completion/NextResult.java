package com.ibm.eventstreams.connect.cossink.completion;

/**
 * The result from testing whether adding a subsequent {@code SinkRecord}
 * to an object will complete the object.
 */
public enum NextResult {

    /**
     * The object is complete, including this record as the last record
     * that makes up the object.
     */
    COMPLETE_INCLUSIVE,

    /**
     * The object is complete and includes all records up to (but not
     * including) this record.
     */
    COMPLETE_NON_INCLUSIVE,

    /**
     * The object is not complete.
     */
    INCOMPLETE
}
