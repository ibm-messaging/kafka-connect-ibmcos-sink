/*
 * Copyright 2019, 2021 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.cossink.deadline;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DeadlineServiceImpl implements DeadlineService {

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(0);

    @Override
    public DeadlineCanceller schedule(DeadlineListener listener, long time, TimeUnit unit, Object context) {
        final Future<?> future = scheduledExecutor.schedule(
            () -> listener.deadlineReached(context),
            time, unit);

        return () -> future.cancel(false);
    }

    @Override
    public void close() {
        scheduledExecutor.shutdownNow();
    }
}
