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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class COSSinkConnectorConfig extends AbstractConfig {

    private static final String CONFIG_GROUP_OS = "os";

    static final String CONFIG_NAME_OS_API_KEY = "os.api.key";
    private static final String CONFIG_DOCUMENTATION_OS_API_KEY =
            "API key for connecting to the Object Storage instance.";
    private static final String CONFIG_DISPLAY_OS_API_KEY = "API key";

    static final String CONFIG_NAME_OS_SERVICE_CRN = "os.service.crn";
    private static final String CONFIG_DOCUMENTATION_OS_SERVICE_CRN =
            "Service CRN for the Object Storage instance.";
    private static final String CONFIG_DISPLAY_OS_SERVICE_CRN  = "Service CRN";

    static final String CONFIG_NAME_OS_BUCKET_LOCATION = "os.bucket.location";
    private static final String CONFIG_DOCUMENTATION_OS_BUCKET_LOCATION =
            "Location of the Object Storage bucket, for example: eu-gb.";
    private static final String CONFIG_DISPLAY_OS_BUCKET_LOCATION = "Bucket location";

    static final String CONFIG_NAME_OS_BUCKET_NAME = "os.bucket.name";
    private static final String CONFIG_DOCUMENTATION_OS_BUCKET_NAME =
            "Name of the Object Storage bucket.";
    private static final String CONFIG_DISPLAY_OS_BUCKET_NAME = "Bucket name";

    static final String CONFIG_NAME_OS_BUCKET_RESILIENCY = "os.bucket.resiliency";
    private static final String CONFIG_DOCUMENTATION_OS_BUCKET_RESILIENCY =
            "Resiliency of the Object Storage bucket, for example: cross-region, regional, or single-site.";
    private static final String CONFIG_DISPLAY_OS_BUCKET_RESILIENCY = "Bucket resiliency";
    static final String CONFIG_VALUE_OS_BUCKET_RESILIENCY_CROSS_REGION = "cross-region";
    static final String CONFIG_VALUE_OS_BUCKET_RESILIENCY_SINGLE_SITE = "single-site";
    static final String CONFIG_VALUE_OS_BUCKET_RESILIENCY_REGIONAL = "regional";

    static final String CONFIG_NAME_OS_ENDPOINT_VISIBILITY = "os.endpoint.visibility";
    private static final String CONFIG_DOCUMENTATION_OS_ENDPOINT_VISIBILITY =
            "Specify 'public' to connect to the Object Storage across the public internet, or 'private' to connect " +
            "using the SoftLayer network.";
    private static final String CONFIG_DISPLAY_OS_ENDPOINT_VISIBILITY = "Endpoint visibility";
    static final String CONFIG_VALUE_OS_ENDPOINT_VISIBILITY_PUBLIC = "public";
    static final String CONFIG_VALUE_OS_ENDPOINT_VISIBILITY_PRIVATE = "private";

    static final String CONFIG_NAME_OS_OBJECT_RECORDS = "os.object.records";
    private static final String CONFIG_DOCUMENTATION_OS_OBJECT_RECORDS =
            "The maximum number of Kafka records to group together into a single object storage object.";
    private static final String CONFIG_DISPLAY_OS_OBJECT_RECORDS = "Records per object";

    static final String CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS = "os.object.deadline.seconds";
    private static final String CONFIG_DOCUMENTATION_OS_OBJECT_DEADLINE_SECONDS =
            "The maximum period of (wall clock) time between the connector receiving a Kafka record and the " +
            "connector writing all of the Kafka records it has received so far into an object storage object.";
    private static final String CONFIG_DISPLAY_OS_OBJECT_DEADLINE_SECONDS = "Object deadline seconds";

    static final String CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS = "os.object.interval.seconds";
    private static final String CONFIG_DOCUMENTATION_OS_OBJECT_INTERVAL_SECONDS =
            "The maximum interval (based on Kafka record timestamp) between the first Kafka record to write into an " +
            "object and the last.";
    private static final String CONFIG_DISPLAY_OS_OBJECT_INTERVAL_SECONDS = "Object interval seconds";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(CONFIG_NAME_OS_API_KEY, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_API_KEY, CONFIG_GROUP_OS, 1, Width.MEDIUM,
                CONFIG_DISPLAY_OS_API_KEY)

        .define(CONFIG_NAME_OS_SERVICE_CRN, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_SERVICE_CRN, CONFIG_GROUP_OS, 2, Width.MEDIUM,
                CONFIG_DISPLAY_OS_SERVICE_CRN)

        .define(CONFIG_NAME_OS_BUCKET_LOCATION, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_BUCKET_LOCATION, CONFIG_GROUP_OS, 3, Width.MEDIUM,
                CONFIG_DISPLAY_OS_BUCKET_LOCATION)

        .define(CONFIG_NAME_OS_BUCKET_NAME, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_BUCKET_NAME, CONFIG_GROUP_OS, 4, Width.MEDIUM,
                CONFIG_DISPLAY_OS_BUCKET_NAME)

        .define(CONFIG_NAME_OS_BUCKET_RESILIENCY, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.ValidString.in(CONFIG_VALUE_OS_BUCKET_RESILIENCY_CROSS_REGION,
                        CONFIG_VALUE_OS_BUCKET_RESILIENCY_SINGLE_SITE,
                        CONFIG_VALUE_OS_BUCKET_RESILIENCY_REGIONAL),
                Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_BUCKET_RESILIENCY, CONFIG_GROUP_OS, 5, Width.MEDIUM,
                CONFIG_DISPLAY_OS_BUCKET_RESILIENCY)

        .define(CONFIG_NAME_OS_ENDPOINT_VISIBILITY, Type.STRING, CONFIG_VALUE_OS_ENDPOINT_VISIBILITY_PUBLIC,
                ConfigDef.ValidString.in(CONFIG_VALUE_OS_ENDPOINT_VISIBILITY_PRIVATE,
                        CONFIG_VALUE_OS_ENDPOINT_VISIBILITY_PUBLIC),
                Importance.LOW,
                CONFIG_DOCUMENTATION_OS_ENDPOINT_VISIBILITY, CONFIG_GROUP_OS, 6, Width.MEDIUM,
                CONFIG_DISPLAY_OS_ENDPOINT_VISIBILITY)

        .define(CONFIG_NAME_OS_OBJECT_RECORDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_OBJECT_RECORDS, CONFIG_GROUP_OS, 7, Width.MEDIUM,
                CONFIG_DISPLAY_OS_OBJECT_RECORDS)

        .define(CONFIG_NAME_OS_OBJECT_DEADLINE_SECONDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_OBJECT_DEADLINE_SECONDS, CONFIG_GROUP_OS, 8, Width.MEDIUM,
                CONFIG_DISPLAY_OS_OBJECT_DEADLINE_SECONDS)

        .define(CONFIG_NAME_OS_OBJECT_INTERVAL_SECONDS, Type.INT, -1, Importance.HIGH,
                CONFIG_DOCUMENTATION_OS_OBJECT_INTERVAL_SECONDS, CONFIG_GROUP_OS, 9, Width.MEDIUM,
                CONFIG_DISPLAY_OS_OBJECT_INTERVAL_SECONDS);

    public COSSinkConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
        super(definition, originals);
    }

    public COSSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(CONFIG_DEF, parsedConfig);
    }

}
