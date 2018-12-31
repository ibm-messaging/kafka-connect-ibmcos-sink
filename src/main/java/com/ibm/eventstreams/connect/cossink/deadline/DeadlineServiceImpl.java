package com.ibm.eventstreams.connect.cossink.deadline;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DeadlineServiceImpl implements DeadlineService {

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(0);

    @Override
    public DeadlineCanceller schedule(DeadlineListener listener, long time, TimeUnit unit, Object context) {
        final Future<?> future = scheduledExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                listener.deadlineReached(context);
            }

        }, time, unit);

        return new DeadlineCanceller() {
            @Override
            public void cancel() {
                future.cancel(false);
            }
        };
    }

    @Override
    public void close() {
        scheduledExecutor.shutdownNow();
    }
}
