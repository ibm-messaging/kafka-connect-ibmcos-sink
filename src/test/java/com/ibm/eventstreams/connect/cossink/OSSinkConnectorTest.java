package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ibm.eventstreams.connect.cossink.OSSinkConnector;
import com.ibm.eventstreams.connect.cossink.OSSinkTask;

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
