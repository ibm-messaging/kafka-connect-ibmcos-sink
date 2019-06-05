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
package com.ibm.eventstreams.connect.cossink.partitionwriter;

import java.util.Deque;
import java.util.LinkedList;

/**
 * RequestProcessor simplifies handling of requests from multiple threads by queueing the requests
 * and delivering them sequentially to the {@code RequestProcessor#process()} method.
 */
abstract class RequestProcessor<T extends Enum<?>> {

    private class Request {
        private final T type;
        private final Object context;

        private Request (T type, Object context) {
            this.type = type;
            this.context = context;
        }
    }

    private final Deque<Request> requests = new LinkedList<>();
    private boolean beingProcessed = false;
    private boolean closed = false;
    private Object monitor = new Object();

    private final T closeValue;

    /**
     * @param closeValue is the enumeration value to pass into the {@code RequestProcessor#process()}
     *           method to indicate that {@code RequestProcessor#close()} has been called.
     */
    RequestProcessor(T closeValue) {
        this.closeValue = closeValue;
    }

    /**
     * Queue a request to be processed. If there is currently a thread running the
     * {@code RequestProcessor#process()} method, then this thread is used to deliver all of
     * the requests from the queue.
     *
     * @param type indicates the type of request.
     * @param context arbitrary request specific data.
     */
    void queue(T type, Object context) {
        synchronized(monitor) {
            if (closed) {
                return;
            }

            requests.add(new Request(type, context));

            if (beingProcessed) {
                return;
            }
            beingProcessed = true;
        }
        process();
    }

    /**
     * Closes the request processor. Any unprocessed requests are discarded. Any further
     * attempts to queue a request are ignored. The "close" enumerated value specified via
     * the constructor is passed to the {@code RequestProcessor#process()} method to notify
     * it of the close.
     */
    void close() {
        synchronized(monitor) {
            closed = true;
            requests.clear();
            requests.add(new Request(closeValue, null));

            if (beingProcessed) {
                return;
            }
            beingProcessed = true;
        }
        process();
    }

    /**
     * Subclasses implement this method to receive requests. Only a single thread will execute
     * this method at any given time, but there is no guarantee that it will be the same thread
     * each time. Any thread entering this method passes through a memory barrier, so variables
     * only accessed in the scope of this method will not result in data races.
     *
     * @param type the type of the request (as specified on the corresponding call to
     *           {@code RequestProcessor#queue(Enum, Object)}.
     *
     * @param context arbitrary data associated with the request.
     */
    abstract void process(T type, Object context);

    private void process() {
        while(true) {
            Request request;
            synchronized(monitor) {
                if (requests.isEmpty()) {
                    beingProcessed = false;
                    return;
                }

                request = requests.remove();
            }

            process(request.type, request.context);
        }
    }

}
