package com.ibm.eventstreams.connect.cossink.deadline;

import java.util.concurrent.TimeUnit;

/**
 * DeadlineService allows a notification to be delivered at some point in the future.
 */
public interface DeadlineService {

    /**
     * Schedule a notification.
     * @param listener the listener to be notified.
     * @param time the time interval after which the notification will be delivered.
     * @param unit the {@code TimeUnit} corresponding to the time parameter.
     * @param context arbitrary data to be delivered as part of the notification.
     * @return a {@code DeadlineCanceller} that can be used to cancel the notification.
     */
    DeadlineCanceller schedule(DeadlineListener listener, long time, TimeUnit unit, Object context);

    /**
     * Close the DeadlineService. Any pending notifications will be discarded.
     */
    void close();

}
