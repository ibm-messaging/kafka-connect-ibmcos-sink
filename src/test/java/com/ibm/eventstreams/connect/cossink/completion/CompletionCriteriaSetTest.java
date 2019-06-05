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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class CompletionCriteriaSetTest {

    @Mock
    ObjectCompletionCriteria criteria1;

    @Mock
    ObjectCompletionCriteria criteria2;

    private CompletionCriteriaSet set;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        set = new CompletionCriteriaSet();
        set.add(criteria1);
        set.add(criteria2);
    }

    // Test the isEmpty() method of the set.
    @Test
    public void isEmpty() {
        set = new CompletionCriteriaSet();
        assertTrue(set.isEmpty());

        set.add(criteria1);
        assertFalse(set.isEmpty());
    }

    // The set delivers the first() event to all registered criteria.
    @Test
    public void firstDeliveredToAllRegisteredCriteria() {
        SinkRecord expectedSinkRecord = Mockito.mock(SinkRecord.class);
        AsyncCompleter expectedAsyncCompleter = Mockito.mock(AsyncCompleter.class);

        set.first(expectedSinkRecord, expectedAsyncCompleter);

        Mockito.verify(criteria1).first(Mockito.eq(expectedSinkRecord), Mockito.eq(expectedAsyncCompleter));
        Mockito.verify(criteria2).first(Mockito.eq(expectedSinkRecord), Mockito.eq(expectedAsyncCompleter));
    }

    // first() returns FirstResult.INCOMPLETE if all registered criteria
    // return FirstResult.INCOMPLETE.
    @Test
    public void firstReturnsIncomplete() {
        Mockito.when(criteria1.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        Mockito.when(criteria2.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        assertEquals(FirstResult.INCOMPLETE,
                set.first(Mockito.mock(SinkRecord.class), Mockito.mock(AsyncCompleter.class)));
    }

    // first() returns FirstResult.COMPLETE if any of the registered criteria
    // return FirstResult.COMPLETE.
    @Test
    public void firstReturnsComplete() {
        Mockito.when(criteria1.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        Mockito.when(criteria2.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.COMPLETE);
        assertEquals(FirstResult.COMPLETE,
                set.first(Mockito.mock(SinkRecord.class), Mockito.mock(AsyncCompleter.class)));
    }

    // The set delivers the next() event to all registered criteria.
    @Test
    public void nextDeliveredToAllRegisteredCriteria() {
        Mockito.when(criteria1.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);
        Mockito.when(criteria2.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);
        SinkRecord expectedSinkRecord = Mockito.mock(SinkRecord.class);

        set.next(expectedSinkRecord);

        Mockito.verify(criteria1).next(Mockito.eq(expectedSinkRecord));
        Mockito.verify(criteria2).next(Mockito.eq(expectedSinkRecord));
    }

    // The set delivers the complete() event to all registered criteria.
    @Test
    public void completeDeliveredToAllRegisteredCriteria() {
        set.complete();

        Mockito.verify(criteria1).complete();
        Mockito.verify(criteria2).complete();
    }

    // The next() method returns Result.INCOMPLETE if all of the criteria return this
    // from their next() methods.
    @Test
    public void nextReturnsIncompleteIfAllCriteriaReturnIncomplete() {
        Mockito.when(criteria1.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);
        Mockito.when(criteria2.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);

        NextResult actualResult = set.next(Mockito.mock(SinkRecord.class));

        assertEquals(NextResult.INCOMPLETE, actualResult);
    }

    // The next() method returns Result.COMPLETE_NON_INCLUSIVE if any of the criteria
    // return this (and regardless of what the other criteria return).
    @Test
    public void nextReturnsCompleteNonInclusiveIfAnyCriteriaReturnIt() {
        ObjectCompletionCriteria criteria3 = Mockito.mock(ObjectCompletionCriteria.class);
        Mockito.when(criteria1.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);
        Mockito.when(criteria2.next(Mockito.any())).thenReturn(NextResult.COMPLETE_NON_INCLUSIVE);
        Mockito.when(criteria3.next(Mockito.any())).thenReturn(NextResult.COMPLETE_INCLUSIVE);

        CompletionCriteriaSet set1 = new CompletionCriteriaSet();
        set1.add(criteria1);
        set1.add(criteria2);

        CompletionCriteriaSet set2 = new CompletionCriteriaSet();
        set2.add(criteria2);
        set2.add(criteria3);

        assertEquals(NextResult.COMPLETE_NON_INCLUSIVE, set1.next(Mockito.mock(SinkRecord.class)));
        assertEquals(NextResult.COMPLETE_NON_INCLUSIVE, set2.next(Mockito.mock(SinkRecord.class)));
    }

    // The next() method returns Result.COMPLETE_INCLUSIVE if any of the criteria
    // return this (and none of the criteria return COMPLETE_NON_INCLUSIVE).
    @Test
    public void nextReturnsCompleteInclusive() {
        Mockito.when(criteria1.next(Mockito.any())).thenReturn(NextResult.INCOMPLETE);
        Mockito.when(criteria2.next(Mockito.any())).thenReturn(NextResult.COMPLETE_INCLUSIVE);
        assertEquals(NextResult.COMPLETE_INCLUSIVE, set.next(Mockito.mock(SinkRecord.class)));
    }
}
