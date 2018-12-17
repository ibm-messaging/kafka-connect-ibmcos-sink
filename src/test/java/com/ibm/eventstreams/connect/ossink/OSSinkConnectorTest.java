package com.ibm.eventstreams.connect.ossink;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OSSinkConnectorTest {

    @Test
    public void version() {
        OSSinkConnector sc = new OSSinkConnector();
        assertEquals(OSSinkConnector.VERSION, sc.version());
    }

    @Test
    public void taskClass() {
        OSSinkConnector sc = new OSSinkConnector();
        assertEquals(OSSinkTask.class, sc.taskClass());
    }

}
