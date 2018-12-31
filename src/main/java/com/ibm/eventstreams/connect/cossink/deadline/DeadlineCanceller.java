package com.ibm.eventstreams.connect.cossink.deadline;

/**
 * DeadlineCanceller allows cancellation of a scheduled deadline.
 */
public interface DeadlineCanceller {

    /**
     * Attempt to cancel. This is best-effort, as the notification that a deadline
     * has been reached may occur on another thread, and there are race conditions
     * whereby cancelling at almost the point the deadline is reached will be
     * ineffective.
     */
    void cancel();

}
