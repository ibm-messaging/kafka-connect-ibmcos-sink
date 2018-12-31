package com.ibm.eventstreams.connect.cossink.deadline;

/**
 * Listener for notifications scheduled via the DeadlineService.
 */
public interface DeadlineListener {

    /**
     * Called when the deadline for a notification is reached.
     * @param context arbitrary data, specified via the
     *             {@code DeadlineService#schedule(DeadlineListener, long, java.util.concurrent.TimeUnit, Object)}
     *             method.
     */
    void deadlineReached(Object context);

}
