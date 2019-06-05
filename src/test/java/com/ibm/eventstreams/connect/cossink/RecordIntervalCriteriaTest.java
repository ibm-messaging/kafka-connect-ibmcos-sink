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
package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.ibm.eventstreams.connect.cossink.completion.AsyncCompleter;
import com.ibm.eventstreams.connect.cossink.completion.FirstResult;
import com.ibm.eventstreams.connect.cossink.completion.NextResult;
import com.ibm.eventstreams.connect.cossink.completion.RecordIntervalCriteria;

public class RecordIntervalCriteriaTest {

    @Mock
    SinkRecord mockSinkRecord;

    @Mock
    AsyncCompleter mockAsyncCompleter;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    // The first() method should always return FirstResult.INCOMPLETE
    @Test
    public void firstReturnsIncomplete() {
        RecordIntervalCriteria ric = new RecordIntervalCriteria(0);
        FirstResult result = ric.first(mockSinkRecord, mockAsyncCompleter);
        assertEquals(FirstResult.INCOMPLETE, result);
    }

    private final SinkRecord sinkRecord(long timestamp) {
        SinkRecord record = Mockito.mock(SinkRecord.class);
        Mockito.when(record.timestamp()).thenReturn(timestamp);
        return record;
    }

    // The next() method should return NextResult.INCOMPLETE until it receives a
    // SinkRecord with a timestamp outside of the interval defined when the
    // RecordIntervalCriteria is constructed. Once this occurs next() should
    // return NextResult.COMPLETE_NON_INCLUSIVE.
    @Test
    public void nextReturnsIncompleteUntilIntervalExceeded() {
        RecordIntervalCriteria ric = new RecordIntervalCriteria(10);
        ric.first(sinkRecord(10000), mockAsyncCompleter);
        assertEquals(NextResult.INCOMPLETE, ric.next(sinkRecord(19999)));
        assertEquals(NextResult.COMPLETE_NON_INCLUSIVE, ric.next(sinkRecord(20000)));
    }
}
