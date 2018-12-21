package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.mockito.Mockito;

import com.ibm.cos.ClientFactory;

public class OSSinkTaskTest {

    @Test
    public void version() throws Exception {
        OSSinkTask task = new OSSinkTask(Mockito.mock(ClientFactory.class));
        assertEquals(OSSinkConnector.VERSION, task.version());
    }

}
