package com.ibm.eventstreams.connect.cossink.partitionwriter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ibm.cloud.objectstorage.AmazonServiceException;
import com.ibm.cloud.objectstorage.SdkClientException;
import com.ibm.cloud.objectstorage.services.s3.model.ObjectMetadata;
import com.ibm.cloud.objectstorage.services.s3.model.PutObjectResult;
import com.ibm.cos.Bucket;
import com.ibm.eventstreams.connect.cossink.completion.AsyncCompleter;
import com.ibm.eventstreams.connect.cossink.completion.CompletionCriteriaSet;
import com.ibm.eventstreams.connect.cossink.completion.FirstResult;
import com.ibm.eventstreams.connect.cossink.completion.NextResult;
import com.ibm.eventstreams.connect.cossink.completion.ObjectCompletionCriteria;

public class OSPartitionWriterTest {

    @Mock ObjectCompletionCriteria criteria;
    private MockBucket mockBucket;
    private CompletionCriteriaSet criteriaSet;
    private OSPartitionWriter writer;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
        mockBucket = new MockBucket();
        criteriaSet = new CompletionCriteriaSet();
        writer = new OSPartitionWriter(mockBucket, criteriaSet);
    }

    private SinkRecord sinkRecord(
            String topic, int partition, byte[] value, int offset) {
        return new SinkRecord(topic, partition, null, null, null, value, offset);
    }

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

    // Calling put() with criteria that return FirstResult.COMPLETE for
    // each record results in an object being written with the contents of
    // the SinkRecord passed into put().
    @Test
    public void putFirstReturnsComplete() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.COMPLETE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));

        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{1}, mockBucket.objects().get(1));
    }

    // Calling put() with criteria that returns FirstResult.INCOMPLETE results
    // in no object being written.
    @Test
    public void putFirstReturnsIncomplete() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));

        assertEquals(0, mockBucket.putCount());
    }

    // Calling put() with a criteria that returns FirstResult.INCOMPLETE and
    // NextResult.COMPLETE_INCLUSIVE will result in objects made up of batches of
    // two sink records.
    @Test
    public void putFirstReturnsIncompleteNextReturnsCompleteInclusive() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        Mockito.when(criteria.next(Mockito.any())).thenReturn(NextResult.COMPLETE_INCLUSIVE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0)); // 1st object
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2)); // 2nd object
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3));
        writer.put(sinkRecord("topic", 0, new byte[]{4}, 4)); // (incomplete) 3rd object

        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{2, 3}, mockBucket.objects().get(1));
    }

    // Calling put() with a criteria that returns FirstResult.INCOMPLETE and
    // NextResult.COMPLETE_NON_INCLUSIVE will result in objects composed of a single
    // sink record. The last sink record put will not be output into an object, as it
    // sits pending a future call to put() to cause it to be written.
    @Test
    public void putFirstReturnsIncompleteNextReturnsCompleteNonInclusive() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        Mockito.when(criteria.next(Mockito.any())).thenReturn(NextResult.COMPLETE_NON_INCLUSIVE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2));

        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{1}, mockBucket.objects().get(1));
    }

    // Calling put() with a criteria that returns FirstResult.INCOMPLETE then
    // FirstResult.COMPLETE and NextResult.COMPLETE_NON_INCLUSIVE will result in
    // objects composed of a single sink record. This is to test the code-path
    // which completes two objects in the second call to put().
    @Test
    public void putFirstReturnsBothNextReturnsCompleteNonInclusive() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(
                FirstResult.INCOMPLETE, FirstResult.COMPLETE);
        Mockito.when(criteria.next(Mockito.any())).thenReturn(NextResult.COMPLETE_NON_INCLUSIVE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));

        assertEquals(2, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
        assertArrayEquals(new byte[]{1}, mockBucket.objects().get(1));
    }

    // Calling put() with a criteria that returns FirstResult.INCOMPLETE then
    // subsequently using the AsyncCompleter to complete the object will result
    // in an object composed of single sink records.
    @Test
    public void useAsyncCompleterToCreateObjects() {
        final AtomicReference<AsyncCompleter> completerRef = new AtomicReference<>();
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).then(
                new Answer<FirstResult>() {
            @Override
            public FirstResult answer(InvocationOnMock invocation) throws Throwable {
                completerRef.set(invocation.getArgumentAt(1, AsyncCompleter.class));
                return FirstResult.INCOMPLETE;
            }
        });
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));
        completerRef.get().asyncComplete();

        assertEquals(1, mockBucket.putCount());
        assertArrayEquals(new byte[]{0}, mockBucket.objects().get(0));
    }

    // If an object is completed via a call to put(), but then the
    // AsyncCompleter.completeAsync() is called, the AsyncCompleter call is
    // ignored, as the object is already complete.
    @Test
    public void asyncCompleterIgnoredIfObjectAlreadyComplete() {
        final AtomicReference<AsyncCompleter> completerRef = new AtomicReference<>();
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).then(
                new Answer<FirstResult>() {
            @Override
            public FirstResult answer(InvocationOnMock invocation) throws Throwable {
                completerRef.set(invocation.getArgumentAt(1, AsyncCompleter.class));
                return FirstResult.INCOMPLETE;
            }
        });
        Mockito.when(criteria.next(Mockito.any())).thenReturn(NextResult.COMPLETE_INCLUSIVE);
        criteriaSet.add(criteria);

        // Write a sink record. This won't complete the current object.
        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));

        // Store current completer value at this point, otherwise it will be overwritten
        // when a new object is started.
        AsyncCompleter oldCompleter = completerRef.get();

        // Write another sink record. This will complete the current object.
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));

        // Writer a further sink record. This shouldn't be part of any object, but
        // could get turned into one, if the AsyncCompleter does the wrong thing
        // and completes the current object.
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2));

        oldCompleter.asyncComplete();

        assertEquals(1, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1}, mockBucket.objects().get(0));
    }

    // Calling put() with a criteria that returns FirstRecord.INCOMPLETE, then
    // NextRecord.INCOMPLETE buffers sink records until a value other than NextRecord.INCOMPLETE
    // is returned, at which point the object is written.
    @Test
    public void putFirstReturnsIncompleteAndNextReturnsIncomplete() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.INCOMPLETE);
        Mockito.when(criteria.next(Mockito.any())).thenReturn(
                NextResult.INCOMPLETE, NextResult.COMPLETE_INCLUSIVE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0)); // 1st object
        writer.put(sinkRecord("topic", 0, new byte[]{1}, 1));
        writer.put(sinkRecord("topic", 0, new byte[]{2}, 2));
        writer.put(sinkRecord("topic", 0, new byte[]{3}, 3)); // 2nd object (incomplete)

        assertEquals(1, mockBucket.putCount());
        assertArrayEquals(new byte[]{0, 1, 2}, mockBucket.objects().get(0));
    }

    // When a call to put() completes an object, the criteria are notified via a
    // call to their complete() methods.
    @Test
    public void criteriaNotifiedWhenPutCompletesObject() {
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.COMPLETE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));

        Mockito.verify(criteria, Mockito.times(1)).complete();
    }

    // When an AsycnCompleter is used to complete an object, the criteria are notified
    // via a call to their complete() methods.
    @Test
    public void criteriaNotifiedWhenAsyncCompleteObject() {
        final AtomicReference<AsyncCompleter> completerRef = new AtomicReference<>();
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).then(
                new Answer<FirstResult>() {
            @Override
            public FirstResult answer(InvocationOnMock invocation) throws Throwable {
                completerRef.set(invocation.getArgumentAt(1, AsyncCompleter.class));
                return FirstResult.INCOMPLETE;
            }
        });
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, 0));
        completerRef.get().asyncComplete();

        Mockito.verify(criteria, Mockito.times(1)).complete();
    }

    // The result from preCommit() should be null if the writer has not yet written any
    // objects.
    @Test
    public void preCommitReturnsNullIfNoObjectsWritten() {
        assertNull(writer.preCommit());
    }

    // Calling preCommit() after an object has been written should return a value
    // which is one more than the offset of the last record that made up the last
    // object written into object storage.
    @Test
    public void preCommitReturnsOneBeyondLastOffset() {
        final int expectedOffset = 10;
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.COMPLETE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, expectedOffset - 2));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, expectedOffset - 1));

        assertEquals(new Long(expectedOffset), writer.preCommit());
    }

    // Calling preCommit() again, before another object has been written should return null to
    // indicate there is no new offset to commit.
    @Test
    public void callingPreCommitBeforeTheNextObjectIsWrittenReturnsNull() {
        final int expectedOffset = 10;
        Mockito.when(criteria.first(Mockito.any(), Mockito.any())).thenReturn(FirstResult.COMPLETE);
        criteriaSet.add(criteria);

        writer.put(sinkRecord("topic", 0, new byte[]{0}, expectedOffset - 2));
        writer.put(sinkRecord("topic", 0, new byte[]{1}, expectedOffset - 1));

        assertEquals(new Long(expectedOffset), writer.preCommit());
        assertNull(writer.preCommit());
    }
}
