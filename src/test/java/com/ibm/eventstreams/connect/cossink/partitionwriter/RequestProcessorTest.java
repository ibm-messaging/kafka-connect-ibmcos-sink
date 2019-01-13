package com.ibm.eventstreams.connect.cossink.partitionwriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

public class RequestProcessorTest {

    private enum TestType {
        CLOSE,
        TEST
    }

    private class TestRequestProcessor extends RequestProcessor<TestType>{

        private final int sleepMin;
        private final int sleepMax;
        private final List<TestType> types = new LinkedList<>();
        private final List<TestRequestContext> contexts = new LinkedList<>();
        private final Random random = new Random();

        private TestRequestProcessor(int sleepMin, int sleepMax) {
            super(TestType.CLOSE);
            this.sleepMin = sleepMin;
            this.sleepMax = sleepMax;
        }

        @Override
        void process(TestType type, Object context) {
            synchronized(types) {
                types.add(type);
            }
            synchronized(contexts) {
                contexts.add((TestRequestContext)context);
            }

            int sleepMillis = sleepMin;
            if (sleepMax > sleepMin) {
                sleepMillis += random.nextInt(sleepMax - sleepMin);
            }

            if (sleepMillis > 0) {
                try {
                    Thread.sleep(sleepMillis);
                } catch(InterruptedException e) {
                }
            }
        }

        List<TestRequestContext> contexts() {
            synchronized(contexts) {
                return contexts;
            }
        }

        List<TestType> types() {
            synchronized(types) {
                return types;
            }
        }
    }

    private class TestRequestContext {
        private final int id;
        private final int sequence;

        private TestRequestContext(int id, int sequence) {
            this.id = id;
            this.sequence = sequence;
        }
    }

    private class TestRequestThread extends Thread {
        private final RequestProcessor<TestType> rp;
        private final int id;
        private final int count;

        private TestRequestThread(RequestProcessor<TestType> rp, int id, int count) {
            this.rp = rp;
            this.id = id;
            this.count = count;
        }

        @Override
        public void run() {
            for (int sequence = 0; sequence < count; sequence++) {
                rp.queue(TestType.TEST, new TestRequestContext(id, sequence));
                if (Math.random() < 0.2) {
                    Thread.yield();
                }
            }
        }
    }

    // For any given thread that generates requests, the requests will be processed in the
    // order in which that thread calls queue().
    @Test
    public void requestsProcessedInOrderQueued() throws Exception {
        final int threadCount = 10;
        final int requestsPerThread = 100;

        TestRequestProcessor rp = new TestRequestProcessor(0, 5);

        final Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new TestRequestThread(rp, i, requestsPerThread);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        final Map<Integer, Integer> results = new HashMap<>();
        for (int i = 0; i < threads.length; i++) {
            results.put(i, -1);
        }

        for (TestRequestContext context : rp.contexts()) {
            int previous = results.get(context.id);
            assertEquals(previous + 1, context.sequence);
            results.put(context.id, context.sequence);
        }
    }


    // Once close() has been called the request processor should discard any new requests.
    @Test
    public void noRequestsProcessedAfterClose() throws Exception {

        TestRequestProcessor rp = new TestRequestProcessor(0, 0);
        rp.close();

        TestRequestThread rt = new TestRequestThread(rp, 0, 1000);
        rt.start();
        rt.join();

        assertEquals(1, rp.types().size());
        assertEquals(TestType.CLOSE, rp.types().get(0));
    }

    // When close() is called, any pending requests are discarded, and the last request that
    // that is processed is a close request.
    @Test
    public void closeDiscardsPendingRequests() throws Exception {
        final int numRequests = 1000;

        TestRequestProcessor rp = new TestRequestProcessor(0, 100);

        TestRequestThread rt = new TestRequestThread(rp, 0, numRequests);
        rt.start();

        Thread.sleep(100);
        rp.close();
        rt.join();

        // Some requests should have been discarded, so the total number of requests
        // recorded will be less than the number sent.
        assertTrue(rp.types().size() < numRequests);

        // Last request received is a close request.
        assertEquals(TestType.CLOSE, rp.types().get(rp.types.size()-1));
    }
}
