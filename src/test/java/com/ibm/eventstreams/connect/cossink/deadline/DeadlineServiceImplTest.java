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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class DeadlineServiceImplTest {

    private DeadlineService service;

    @Before
    public void setUp() {
        service = new DeadlineServiceImpl();
    }

    @After
    public void tearDown() {
        service.close();
    }

    @Test
    public void testSchedule() {
        Object context = new Object();
        DeadlineListener listener = mock(DeadlineListener.class);
        service.schedule(listener, 1, TimeUnit.SECONDS, context);
        verify(listener, timeout(2000).times(1)).deadlineReached(context);
    }

    @Test
    public void testCancel() {
        Object context = new Object();
        DeadlineListener listener = mock(DeadlineListener.class);
        DeadlineCanceller canceller = service.schedule(listener, 1, TimeUnit.SECONDS, context);
        canceller.cancel();
        verify(listener, after(2000).never()).deadlineReached(context);
    }
}
