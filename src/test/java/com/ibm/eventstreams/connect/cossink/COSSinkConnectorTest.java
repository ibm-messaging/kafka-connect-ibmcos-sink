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
package com.ibm.eventstreams.connect.cossink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Test;

public class COSSinkConnectorTest {

    // Test that the connector returns the version number encoded in its version
    // constant.
    @Test
    public void version() {
        COSSinkConnector sc = new COSSinkConnector();
        assertEquals(COSSinkConnector.VERSION, sc.version());
    }

    // Test that the connector returns a task class that implements SinkTask.
    @Test
    public void taskClass() {
        COSSinkConnector sc = new COSSinkConnector();
        Class<?> clazz = sc.taskClass();
        assertTrue(SinkTask.class.isAssignableFrom(clazz));
    }

    // Verify that the config passed in to the connector's start() method is
    // propagated to the task configs returned by taskConfigs()
    @Test
    public void configPropagated() {
        Map<String, String> config = new HashMap<>();
        config.put("test.key", "test.value");

        COSSinkConnector sc = new COSSinkConnector();
        sc.start(config);
        List<Map<String, String>> taskConfigs = sc.taskConfigs(1);

        assertEquals(1, taskConfigs.size());
        assertEquals(config, taskConfigs.get(0));
    }

    // taskConfigs should always return the requested number of config maps
    // entries in the list it returns.
    @Test
    public void taskConfigsReturnsRequestedNumberOfConfigs() {
        COSSinkConnector sc = new COSSinkConnector();
        sc.start(new HashMap<>());

        for (int i=0; i < 10; i++) {
            assertEquals(i, sc.taskConfigs(i).size());
        }
    }
}
