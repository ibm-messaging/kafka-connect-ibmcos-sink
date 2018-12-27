package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.cos.ClientFactory;
import com.ibm.eventstreams.connect.cossink.partitionwriter.PartitionWriterFactory;

public class OSSinkTaskTest {

    @Test
    public void version() throws Exception {
        OSSinkTask task = new OSSinkTask(
                Mockito.mock(ClientFactory.class), Mockito.mock(PartitionWriterFactory.class));

        assertEquals(OSSinkConnector.VERSION, task.version());
    }

}
