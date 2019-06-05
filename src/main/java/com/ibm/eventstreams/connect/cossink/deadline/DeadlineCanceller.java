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
