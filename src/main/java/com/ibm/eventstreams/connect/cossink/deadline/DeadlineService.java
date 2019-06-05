/*
 * Copyright 2019 IBM Corporation
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
