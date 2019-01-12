package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ibm.cos.Client;
import com.ibm.cos.ClientFactory;
import com.ibm.eventstreams.connect.cossink.deadline.DeadlineService;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriter;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriterFactory;

public class OSSinkTaskTest {

    @Mock
    ClientFactory mockClientFactory;

    @Mock
    PartitionWriterFactory mockPartitionWriterFactory;

    @Mock
    DeadlineService mockDeadlineService;

    private Map<TopicPartition, PartitionWriter> assignedWriters;

    private OSSinkTask task;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);

        Mockito.when(mockPartitionWriterFactory.newPartitionWriter(Mockito.any(), Mockito.any()))
        .thenAnswer(new Answer<PartitionWriter>() {
            @Override
            public PartitionWriter answer(InvocationOnMock invocation) throws Throwable {
                return Mockito.mock(PartitionWriter.class);
            }
        });

        Mockito.when(
                mockClientFactory.newClient(
                        Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
        .thenAnswer(new Answer<Client>() {
            @Override
            public Client answer(InvocationOnMock invocation) throws Throwable {
                return Mockito.mock(Client.class);
            }
        });

        assignedWriters = new HashMap<>();

        task = new OSSinkTask(
                mockClientFactory, mockPartitionWriterFactory, assignedWriters, mockDeadlineService);
    }

    // Comparing collections is error prone as both List and Set implement Collection, but
    // lists are not equal to sets and vice-versa. Normalize by converting both collections to sets.
    private <T> void assertCollectionsEqual(Collection<T> expected, Collection<T> actual) {
        assertEquals(new HashSet<T>(expected), new HashSet<T>(actual));
    }

    // OSSinkTask returns the same version number as the connector.
    @Test
    public void version() throws Exception {
        OSSinkConnector sc = new OSSinkConnector();

        assertEquals(sc.version(), task.version());
    }

    // Calling start() should create a COS bucket
    @Test
    public void startCreatesBucket() {
        task.initialize(Mockito.mock(SinkTaskContext.class));

        Map<String, String> config = new HashMap<>();
        config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS, "1");
        config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS, "-1");
        config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS, "-1");
        task.start(config);

        Mockito.verify(mockClientFactory, Mockito.atLeastOnce()).newClient(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    // Calling start() should assign any partitions initially present in the context to the task.
    @Test
    public void startAssignsInitialPartitions() {
        List<TopicPartition> tpList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            Set<TopicPartition> tp = new HashSet<>(tpList);
            SinkTaskContext mockContext = Mockito.mock(SinkTaskContext.class);
            Mockito.when(mockContext.assignment()).thenReturn(tp);

            task.initialize(mockContext);

            Map<String, String> config = new HashMap<>();
            config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS, "1");
            config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS, "-1");
            config.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS, "-1");
            task.start(config);

            Collection<TopicPartition> assignedTp = assignedWriters.keySet();
            assertCollectionsEqual(tp, assignedTp);

            tpList.add(new TopicPartition("topic", i));
        }
    }

    // Calling open() when the task has no assigned partitions should assign
    // the partitions specified on the open call.
    @Test
    public void openAssignsPartitions() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        Set<TopicPartition> expected = new HashSet<>();
        expected.add(tp0);
        expected.add(tp1);

        task.open(expected);
        Collection<TopicPartition> actual = assignedWriters.keySet();

        assertCollectionsEqual(expected, actual);
    }

    // Calling open() when a task has assigned partitions should add partitions
    // to the assignment
    @Test
    public void openAdditionalPartitions() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        Set<TopicPartition> expected = new HashSet<>();
        expected.add(tp0);
        expected.add(tp1);

        task.open(Collections.singleton(tp0));
        task.open(Collections.singleton(tp1));
        Collection<TopicPartition> actual = assignedWriters.keySet();

        assertCollectionsEqual(expected, actual);
    }

    // Calling open() with a partition that is already assigned is a no-op.
    // Specifically: no new PartitionWriter is created.
    @Test
    public void openSamePartitionMultipleTimes() {
        TopicPartition tp0 = new TopicPartition("topic", 0);

        task.open(Collections.singleton(tp0));
        PartitionWriter pw = assignedWriters.get(tp0);

        task.open(Collections.singleton(tp0));

        Collection<TopicPartition> actual = assignedWriters.keySet();
        assertCollectionsEqual(Collections.singleton(tp0), actual);

        assertSame(pw, assignedWriters.get(tp0));
    }

    // Calling close() should remove partitions from the assignment.
    @Test
    public void closeRemovesAssignedPartitions() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        Set<TopicPartition> assigned = new HashSet<>();
        assigned.add(tp0);
        assigned.add(tp1);

        task.open(assigned);
        task.close(Collections.singleton(tp0));

        assigned.remove(tp0);

        Collection<TopicPartition> actual = assignedWriters.keySet();
        assertEquals(new HashSet<>(assigned), new HashSet<>(actual));
    }

    // When a partition is closed, its associated PartitionWriter is
    // also closed.
    @Test
    public void closeAlsoClosesPartitionWriter() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        task.open(Collections.singleton(tp0));

        PartitionWriter pw = assignedWriters.get(tp0);
        Mockito.verify(pw, Mockito.never()).close();

        task.close(Collections.singleton(tp0));
        Mockito.verify(pw, Mockito.times(1)).close();
    }

    // A second, duplicate, call to close a partition is ignored.
    @Test
    public void closeIgnoresDuplicateClosesOfAPartition() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);

        task.open(Collections.singleton(tp0));
        task.open(Collections.singleton(tp1));

        task.close(Collections.singleton(tp0));
        task.close(Collections.singleton(tp0));

        assertCollectionsEqual(Collections.singleton(tp1), assignedWriters.keySet());
    }

    // Put forwards sync-records to the appropriate partition writers, in order.
    @Test
    public void putForwardsToPartitionWriters() {
        SinkRecord sr0 = new SinkRecord("topic", 0, null, null, null, null, 0);
        SinkRecord sr1 = new SinkRecord("topic", 1, null, null, null, null, 0);
        SinkRecord sr2 = new SinkRecord("topic", 0, null, null, null, null, 1);
        List<SinkRecord> sinkRecords = new ArrayList<>();
        sinkRecords.add(sr0);
        sinkRecords.add(sr1);
        sinkRecords.add(sr2);

        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        task.open(Collections.singleton(tp0));
        task.open(Collections.singleton(tp1));

        PartitionWriter pw0 = assignedWriters.get(tp0);
        PartitionWriter pw1 = assignedWriters.get(tp1);

        task.put(sinkRecords);

        InOrder inOrder = Mockito.inOrder(pw0);
        inOrder.verify(pw0).put(Mockito.eq(sr0));
        inOrder.verify(pw0).put(Mockito.eq(sr2));

        Mockito.verify(pw1).put(Mockito.eq(sr1));
    }

    // Attempting to put a SinkRecord to a partition which is not currently assigned to
    // the sink task is ignored.
    @Test
    public void putIgnoresUnassignedPartitions() {
        SinkRecord sr0 = new SinkRecord("topic", 0, null, null, null, null, 0);
        SinkRecord sr1 = new SinkRecord("topic", 1, null, null, null, null, 0);
        SinkRecord sr2 = new SinkRecord("topic", 0, null, null, null, null, 1);
        List<SinkRecord> sinkRecords = new ArrayList<>();
        sinkRecords.add(sr0);
        sinkRecords.add(sr1);
        sinkRecords.add(sr2);

        TopicPartition tp0 = new TopicPartition("topic", 0);
        task.open(Collections.singleton(tp0));

        PartitionWriter pw0 = assignedWriters.get(tp0);

        task.put(sinkRecords);

        InOrder inOrder = Mockito.inOrder(pw0);
        inOrder.verify(pw0).put(Mockito.eq(sr0));
        inOrder.verify(pw0).put(Mockito.eq(sr2));
    }

    // The preCommit method delegates preCommit calls to all assigned PartitionWriters.
    // and reports their results.
    @Test
    public void preCommitDelegatesToPartitionWriters() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        task.open(Collections.singleton(tp0));
        task.open(Collections.singleton(tp1));

        PartitionWriter pw0 = assignedWriters.get(tp0);
        PartitionWriter pw1 = assignedWriters.get(tp1);

        Mockito.when(pw0.preCommit()).thenReturn(1L);
        Mockito.when(pw1.preCommit()).thenReturn(2L);

        Map<TopicPartition, OffsetAndMetadata> expected = new HashMap<>();
        expected.put(tp0, new OffsetAndMetadata(1L));
        expected.put(tp1, new OffsetAndMetadata(2L));

        assertEquals(expected, task.preCommit(null));
    }

    // A PartitionWriter can return null to indicate that it has no updated offset to
    // commit. In this case, it will be omitted from the result returned by the sink task's
    // preCommit method.
    @Test
    public void preCommitSkipsParitionWritersThatHaveMadeNoProgress() {
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        task.open(Collections.singleton(tp0));
        task.open(Collections.singleton(tp1));

        PartitionWriter pw0 = assignedWriters.get(tp0);
        PartitionWriter pw1 = assignedWriters.get(tp1);

        Mockito.when(pw0.preCommit()).thenReturn(1L);
        Mockito.when(pw1.preCommit()).thenReturn(null);

        Map<TopicPartition, OffsetAndMetadata> expected = new HashMap<>();
        expected.put(tp0, new OffsetAndMetadata(1L));

        assertEquals(expected, task.preCommit(null));
    }

    // At least one of 'os.object.records', 'os.object.deadline.seconds', or 'os.object.interval.seconds'
    // must have a value which is greater than zero. This is validated in the start() method.
    @Test(expected=ConfigException.class)
    public void startThrowsConfigExceptionIfObjectSizeConfigAllUnset() {
        Map<String, String> props = new HashMap<>();
        props.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_RECORDS, "-1");
        props.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS, "-1");
        props.put(OSSinkConnectorConfig.CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS, "-1");
        task.start(props);
    }

    // Closing the SinkTask should also close the instance of the DeadlineService
    // that the task uses.
    @Test
    public void closeClosesDeadlineService() {
        task.close(new HashSet<TopicPartition>());
        Mockito.verify(mockDeadlineService).close();
    }

}
