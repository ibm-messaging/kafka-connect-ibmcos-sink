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
package com.ibm.eventstreams.connect.cossink.completion;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.eventstreams.connect.cossink.deadline.DeadlineCanceller;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineListener;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;

public class DeadlineCriteriaTest {

    private class MockDeadlineService implements DeadlineService {

        private DeadlineListener lastListener;
        private Object lastContext;
        private DeadlineCanceller lastCanceller;

        @Override
        public DeadlineCanceller schedule(
                DeadlineListener listener, long time, TimeUnit unit, Object context) {
            this.lastListener = listener;
            this.lastContext = context;
            this.lastCanceller = Mockito.mock(DeadlineCanceller.class);
            return this.lastCanceller;
        }

        @Override
        public void close() {}

        DeadlineListener lastListener() {
            return lastListener;
        }

        Object lastContext() {
            return lastContext;
        }

        DeadlineCanceller lastCanceller() {
            return lastCanceller;
        }
    }

    // Calling first() schedules a deadline using the configured deadline service
    // and with the configured deadline time.
    @Test
    public void firstSchedulesDeadline() {
        final int expectedDeadlineSec = 10;
        DeadlineService mockDeadlineService = Mockito.mock(DeadlineService.class);
        DeadlineCriteria dc = new DeadlineCriteria(mockDeadlineService, expectedDeadlineSec);

        dc.first(Mockito.mock(SinkRecord.class), Mockito.mock(AsyncCompleter.class));

        Mockito.verify(mockDeadlineService).schedule(
                Mockito.any(), Mockito.eq((long)expectedDeadlineSec),
                Mockito.eq(TimeUnit.SECONDS), Mockito.any());
    }

    // Calling first() always returns FirstResult.INCOMPLETE.
    @Test
    public void firstAlwaysReturnsIncomplete() {
        DeadlineCriteria dc = new DeadlineCriteria(Mockito.mock(DeadlineService.class), 0);
        SinkRecord mockSinkRecord = Mockito.mock(SinkRecord.class);
        AsyncCompleter mockCompleter = Mockito.mock(AsyncCompleter.class);

        for (int i = 0; i < 1000; i++) {
            FirstResult actualResult =  dc.first(mockSinkRecord, mockCompleter);
            assertEquals(FirstResult.INCOMPLETE, actualResult);
        }
    }

    // If a deadline, scheduled by the first() method is reached then the instance
    // of AsyncCompleter passed in to the first() method is called to complete the
    // object.
    @Test
    public void asyncCompleteCalledIfDeadlineReached() {
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        DeadlineCriteria dc = new DeadlineCriteria(mockDeadlineService, 0);

        AsyncCompleter mockCompleter = Mockito.mock(AsyncCompleter.class);
        dc.first(Mockito.mock(SinkRecord.class), mockCompleter);

        mockDeadlineService.lastListener().deadlineReached(mockDeadlineService.lastContext());

        Mockito.verify(mockCompleter).asyncComplete();
    }

    // Calling complete() cancels the current deadline.
    @Test
    public void completeCancelsDeadline() {
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        DeadlineCriteria dc = new DeadlineCriteria(mockDeadlineService, 0);

        dc.first(Mockito.mock(SinkRecord.class), Mockito.mock(AsyncCompleter.class));
        dc.complete();

        Mockito.verify(mockDeadlineService.lastCanceller()).cancel();
    }

    // Calling next() always returns NextResult.INCOMPLETE as the criteria completes objects
    // asynchronously via the AsyncCompleter.
    @Test
    public void nextAlwaysReturnsIncomplete() {
        DeadlineCriteria dc = new DeadlineCriteria(Mockito.mock(DeadlineService.class), 0);
        dc.first(Mockito.mock(SinkRecord.class), Mockito.mock(AsyncCompleter.class));

        SinkRecord mockSinkRecord = Mockito.mock(SinkRecord.class);
        for (int i = 0; i < 1000; i++) {
            assertEquals(NextResult.INCOMPLETE, dc.next(mockSinkRecord));
        }
    }
}
