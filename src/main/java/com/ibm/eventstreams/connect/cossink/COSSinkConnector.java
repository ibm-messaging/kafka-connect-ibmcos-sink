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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class COSSinkConnector extends SinkConnector {

    private static final Logger LOG = LoggerFactory.getLogger(COSSinkConnector.class);

    static final String VERSION = "1.0.0";

    private Map<String, String> config;

    /**
     * Get the version of this component.
     *
     * @return the version, formatted as a String. The version may not be (@code null} or empty.
     */
    @Override
    public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        LOG.trace("> start, props={}", props);
        this.config = props;
        LOG.trace("< start");
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return COSSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOG.trace("> taskConfigs, maxTasks={}", maxTasks);
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(config);
        }
        LOG.trace("< taskConfigs, retval={}", taskConfigs);
        return taskConfigs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
        LOG.trace("> stop");
        LOG.trace("< stop");
    }

    /**
     * Define the configuration for the connector.
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        return COSSinkConnectorConfig.CONFIG_DEF;
    }

}
