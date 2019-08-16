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

import static org.junit.Assert.assertEquals;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class COSObjectTest {

    @Test
    public void testCreateKey() {
        COSObject object = new COSObject(null);
        object.put(new SinkRecord("topic", 7, null, null, null, null, 0, null, null, null));
        assertEquals("topic/7/0000000000000000-0000000000000000", object.createKey());

        object.put(new SinkRecord("topic", 7, null, null, null, null, 1, null, null, null));
        object.put(new SinkRecord("topic", 7, null, null, null, null, 2, null, null, null));
        assertEquals("topic/7/0000000000000000-0000000000000002", object.createKey());

        object = new COSObject(null);
        object.put(new SinkRecord("topic", 111, null, null, null, null, 1234567890123456L, null, null, null));
        object.put(new SinkRecord("topic", 111, null, null, null, null, 1234567890123457L, null, null, null));
        assertEquals("topic/111/1234567890123456-1234567890123457", object.createKey());
    }

    @Test
    public void testRecordDelimiter() {
        COSObject object = new COSObject("\n");


        object.put(new SinkRecord("topic", 7, null, null, null, "one".getBytes(), 1, null, null, null));
        object.put(new SinkRecord("topic", 7, null, null, null, "two".getBytes(), 2, null, null, null));
        assertEquals(java.util.Arrays.toString("one\ntwo\n".getBytes()), java.util.Arrays.toString(object.createStream()));
    }

}
