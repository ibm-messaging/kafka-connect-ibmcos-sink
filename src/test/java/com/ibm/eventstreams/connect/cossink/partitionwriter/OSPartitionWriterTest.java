package com.ibm.eventstreams.connect.cossink.partitionwriter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.cloud.objectstorage.AmazonServiceException;
import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineCanceller;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineListener;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;


public class OSPartitionWriterTest {

    private class MockBucket implements Bucket {

        private int putCount;
        private List<byte[]> objects = new LinkedList<>();

        @Override
        public PutObjectResult putObject(
                String key, InputStream input, ObjectMetadata metadata)
                throws SdkClientException, AmazonServiceException {
            putCount++;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while(true) {
                try {
                    int b = input.read();
                    if (b < 0) {
                        break;
                    }
                    baos.write((byte)b);
                } catch(IOException e) {
                }
            }
            objects.add(baos.toByteArray());
            return null;
        }

        private int putCount() {
            return putCount;
        }

        private List<byte[]> objects() {
            return objects;
        }
    }

    private class MockDeadlineService implements DeadlineService {

        private final List<Object> contexts = new LinkedList<>();
        private DeadlineCanceller lastCanceller;

        @Override
        public DeadlineCanceller schedule(DeadlineListener listener, long time, TimeUnit unit, Object context) {
            contexts.add(context);
            lastCanceller = Mockito.mock(DeadlineCanceller.class);
            return lastCanceller;
        }

        @Override
        public void close() {}

        private boolean hasContext() {
            return !contexts.isEmpty();
        }

        private Object popContext() {
            assertFalse(contexts.isEmpty());
            return contexts.remove(contexts.size()-1);
        }

        private DeadlineCanceller lastCanceller() {
            return lastCanceller;
        }
    }

    // When only the 'os.object.records' property is set, objects are written
    // at the point the record count is met.
    @Test
    public void objectWrittenWhenRecordCountReached() {
        final int objectRecords = 3;

        MockBucket mockBucket = new MockBucket();
        DeadlineService mockDeadlineService = Mockito.mock(DeadlineService.class);
        OSPartitionWriter writer = new OSPartitionWriter(
                -1, -1, objectRecords, mockBucket, mockDeadlineService);

        for (int i = 0; i < objectRecords * 10; i++) {
            writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)i}, i));
        }

        assertEquals(10, mockBucket.putCount());

        int count = 0;
        for (byte[] data : mockBucket.objects()) {
            assertEquals(objectRecords, data.length);
            for (byte b : data) {
                assertEquals(count, b);
                count++;
            }
        }
    }

    // When only the 'os.object.deadline.seconds' property is set, objects are
    // written at the point the deadline is reached.
    @Test
    public void objectWrittenWhenDeadlineReached() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, -1, mockBucket, mockDeadlineService);

        for (int i = 0; i < 5; i++) {
            writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)i}, i));
        }

        assertEquals(0, mockBucket.putCount);

        Object context = mockDeadlineService.popContext();
        writer.deadlineReached(context);

        assertEquals(1, mockBucket.putCount);
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, mockBucket.objects().get(0));
    }

    // When using 'os.object.deadline.seconds' verify that the deadline is started at the
    // point the first record that will make up a new object is received.
    @Test
    public void deadlineStartsAtFirstRecordForNewObject() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, -1, mockBucket, mockDeadlineService);

        for (int i = 0; i < 50; i++) {
            if (i % 5 == 0) {
                if (i > 0) {
                    writer.deadlineReached(mockDeadlineService.popContext());
                }
                assertFalse(mockDeadlineService.hasContext());
            } else {
                assertTrue(mockDeadlineService.hasContext());
            }
            writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)i}, i));
        }
    }

    private SinkRecord sinkRecord(
            String topic, int partition, byte[] value, int offset) {
            return new SinkRecord(topic, partition, null, null, null, value, offset);
    }

    private SinkRecord sinkRecord(
            String topic, int partition, byte[] value, int offset, long timestamp) {
        return new SinkRecord(
                topic, partition, null, null, null, value, offset,
                timestamp, TimestampType.CREATE_TIME);
    }

    // When only the 'os.object.interval.seconds' property is set, objects are written
    // at the point the point the interval is met.
    @Test
    public void objectwrittenWhenIntervalReached() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                -1, 5, -1, mockBucket, mockDeadlineService);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0, 10000));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1, 11000));    // one second later
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2, 13000));    // three seconds later
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3, 14999));    // 4.999 seconds later
        writer.put(sinkRecord("topic", 0, new byte[]{4}, 4, 15000));    // 5 seconds later (new object)
        writer.put(sinkRecord("topic", 0, new byte[]{5}, 5, 30000));    // 20 seconds (new object)

        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1, 2, 3}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{4}, mockBucket.objects().get(1));
    }

    // If both the 'os.object.records' and 'os.object.deadline.seconds' properties are set
    // then if an object is completed because there are sufficient records, the deadline should
    // be cancelled.
    @Test
    public void deadlineCancelledIfObjectStoredBecauseOfRecordCount() {
        final int objectRecords = 3;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, objectRecords, mockBucket, mockDeadlineService);

        // Write enough records for one object to be written.
        for (int i = 0; i < objectRecords; i++) {
            writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)i}, i));
        }

        Mockito.verify(mockDeadlineService.lastCanceller()).cancel();
    }

    // If both the 'os.object.deadline.seconds' and 'os.object.interval.seconds' properties are set
    // then if an object is completed because the interval has been met, the deadline should
    // be cancelled.
    @Test
    public void deadlineCancelledIfIntervalMetFirst() {
        final int recordInterval = 3;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, recordInterval, -1, mockBucket, mockDeadlineService);

        // Write two records, far enough apart, that it causes an object to be
        // written into object storage. Store the canceller after writing the first
        // record, as writing the second record will complete the first object and
        // create a new canceller for the next object.
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0, 10000));
        DeadlineCanceller canceller = mockDeadlineService.lastCanceller();
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1, 10000 + (recordInterval * 1000)));

        // Canceller associated with first object should be cancelled.
        Mockito.verify(canceller).cancel();

        // Canceller associated with object that is currently being built should not
        // be cancelled.
        Mockito.verify(mockDeadlineService.lastCanceller(), Mockito.never()).cancel();
    }

    // If both the 'os.object.records' and 'os.object.deadline.seconds' properties
    // are set then it's possible for the deadline reached notification to be queued
    // up behind one of the put request that completes the object. Check that this
    // notification is ignored, as it applies to an object that has already been stored.
    @Test
    public void deadlineIgnoredIfObjectStoredDueToRecordCount() {
        final int objectRecords = 3;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, objectRecords, mockBucket, mockDeadlineService);

        // Write enough records for one object to be written.
        for (int i = 0; i < objectRecords; i++) {
            writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)i}, i));
        }

        // Make a note of the context that the deadline service will call back using.
        Object context = mockDeadlineService.popContext();

        // Add one more record. This shouldn't be written, but if there is an error in the
        // code for disregarding deadlines on already written objects, it might be.
        writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)objectRecords}, objectRecords));

        // Notify the writer that the (old) deadline has been reached.
        writer.deadlineReached(context);

        // Only one object should have been written, the deadline should have been ignored
        // and should not have caused a second object to be written.
        assertEquals(1, mockBucket.putCount);
        assertArrayEquals(new byte[]{0, 1, 2}, mockBucket.objects().get(0));
    }

    // If both the 'os.object.deadline.seconds' and 'os.object.interval.seconds' properties
    // are set then it's also possible for the deadline reached notification to be queued
    // up behind one of the put request that completes the object. Check that this
    // notification is ignored, as it applies to an object that has already been stored.
    @Test
    public void deadlineIgnoredIfObjectStoredDueToInterval() {
        final int objectInterval = 5;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, objectInterval, 0, mockBucket, mockDeadlineService);

        // Write enough records for one object to be written and make a note
        // of the context relating to the deadline for this object.
        // create a new canceller for the next object.
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0, 10000));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1, 11000));
        Object context = mockDeadlineService.popContext();
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2, 10000 + (objectInterval * 1000)));

        // Notify the writer that the (old) deadline has been reached.
        writer.deadlineReached(context);

        // Only one object should have been written, the deadline should have been ignored
        // and should not have caused a second object to be written.
        assertEquals(1, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1}, mockBucket.objects().get(0));
    }

    // If both 'os.object.deadline.seconds' and 'os.object.records' are set then the object
    // is stored at the point the first of these two criteria is met.
    @Test
    public void objectStoredWhenEnoughRecordsOrDeadlineMet() {
        final int objectRecords = 3;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, objectRecords, mockBucket, mockDeadlineService);

        // Write a record
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));

        // Notify the writer that the deadline was reached
        writer.deadlineReached(mockDeadlineService.popContext());

        // Write enough records for the 'os.object.records' value to be reached.
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2));
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3));

        // Two objects should have been written to object storage
        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{1, 2, 3}, mockBucket.objects().get(1));
    }

    // If both 'os.object.interval.seconds' and 'os.object.records' are set then the object
    // is stored at the point the first of these two criteria is met.
    @Test
    public void objectStoredWhenEnoughRecordsOrIntervalMet() {
        final int objectInterval = 5;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                -1, objectInterval, 5, mockBucket, mockDeadlineService);

        // Write enough records for the 'os.object.records' to be reached
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0, 10000));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1, 10200));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2, 10400));
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3, 10600));
        writer.put(sinkRecord("topic", 0, new byte[]{4}, 4, 10800));

        // Write enough records for the 'os.object.interval.seconds' value to be reached.
        writer.put(sinkRecord("topic", 0, new byte[]{5}, 5, 11000));
        writer.put(sinkRecord("topic", 0, new byte[]{6}, 6, 13000));
        writer.put(sinkRecord("topic", 0, new byte[]{7}, 7, 16000));

        // Two objects should have been written to object storage
        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{5, 6}, mockBucket.objects().get(1));
    }

    // If both 'os.object.deadline.seconds' and 'os.object.interval.seconds' are set then
    // the object is stored at the point the first of these two criteria is met.
    @Test
    public void objectStoredWhenDeadlineOrIntervalMet() {
        final int objectInterval = 5;

        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, objectInterval, -1, mockBucket, mockDeadlineService);

        // Write a record
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0, 10000));

        // Notify the writer that the deadline was reached
        writer.deadlineReached(mockDeadlineService.popContext());

        // Write enough records for the 'os.object.interval.seconds' value to be reached.
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1, 11000));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2, 13000));
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3, 16000));

        // Two objects should have been written to object storage
        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{1, 2}, mockBucket.objects().get(1));
    }

    // The result from preCommit() should be null if the writer has not yet written any
    // objects.
    @Test
    public void preCommitReturnsNullIfNoObjectsWritten() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, -1, mockBucket, mockDeadlineService);
        assertNull(writer.preCommit());
    }

    // Calling preCommit() after an object has been written should return one past the offset of
    // the last record that made up the object.
    @Test
    public void preCommitReturnsLastOffsetPlusOne() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, -1, mockBucket, mockDeadlineService);

        // Write an object containing two records, with the last record being at offset 7.
        final long lastOffset = 7;
        writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)0x00}, lastOffset-1));
        writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)0x00}, lastOffset));
        writer.deadlineReached(mockDeadlineService.popContext());

        assertEquals(lastOffset + 1, (long)writer.preCommit());
    }

    // Calling preCommit() again, before another object has been written should return null to
    // indicate there is no new offset to commit.
    @Test
    public void callingPreCommitBeforeTheNextObjectIsWrittenReturnsNull() {
        MockBucket mockBucket = new MockBucket();
        MockDeadlineService mockDeadlineService = new MockDeadlineService();
        OSPartitionWriter writer = new OSPartitionWriter(
                10, -1, -1, mockBucket, mockDeadlineService);

        writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)0x00}, 0));
        writer.deadlineReached(mockDeadlineService.popContext());

        assertNotNull(writer.preCommit());
        assertNull(writer.preCommit());

        writer.put(new SinkRecord("topic", 0, null, null, null, new byte[]{(byte)0x00}, 1));
        writer.deadlineReached(mockDeadlineService.popContext());

        assertNotNull(writer.preCommit());
        assertNull(writer.preCommit());
    }

}
