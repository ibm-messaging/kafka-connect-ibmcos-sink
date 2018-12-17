package com.ibm.eventstreams.connect.ossink;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OSSinkTaskTest {

    @Test
    public void version() {
        OSSinkTask task = new OSSinkTask();
        assertEquals(OSSinkConnector.VERSION, task.version());
    }
}
