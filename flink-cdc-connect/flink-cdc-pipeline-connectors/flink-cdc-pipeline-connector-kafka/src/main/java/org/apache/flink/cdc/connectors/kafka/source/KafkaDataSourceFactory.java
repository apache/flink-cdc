/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kafka.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.kafka.json.JsonDeserializationType;
import org.apache.flink.cdc.connectors.kafka.json.canal.CanalJsonDeserializationSchema;
import org.apache.flink.table.api.ValidationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.DATABASE_NAME;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.GROUP_ID;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCHEMA_INFER_ENABLED;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.SCHEMA_NAME;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.TABLE_NAME;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.TOPIC;
import static org.apache.flink.cdc.connectors.kafka.source.KafkaDataSourceOptions.VALUE_FORMAT;

/** A {@link Factory} to create {@link KafkaDataSource} for reading from Kafka topics. */
@Internal
public class KafkaDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "kafka";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper helper = FactoryHelper.createFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);

        org.apache.flink.cdc.common.configuration.Configuration config =
                context.getFactoryConfiguration();

        // Validate required options
        String topic = config.get(TOPIC);
        if (topic == null) {
            throw new ValidationException("'topic' is required for Kafka source.");
        }

        // Extract Kafka properties from the configuration
        Properties kafkaProperties = new Properties();
        Map<String, String> allOptions = config.toMap();
        allOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = allOptions.get(key);
                            final String subKey = key.substring(PROPERTIES_PREFIX.length());
                            kafkaProperties.put(subKey, value);
                        });

        if (!kafkaProperties.containsKey("bootstrap.servers")) {
            throw new ValidationException(
                    "'properties.bootstrap.servers' is required for Kafka source.");
        }

        String groupId = config.get(GROUP_ID);
        if (groupId == null) {
            throw new ValidationException("'properties.group.id' is required for Kafka source.");
        }

        // Validate value format
        JsonDeserializationType valueFormat = config.get(VALUE_FORMAT);
        if (valueFormat != JsonDeserializationType.CANAL_JSON) {
            throw new ValidationException(
                    "Kafka source currently only supports 'canal-json' as value.format, but was: "
                            + valueFormat);
        }

        // Get startup mode and related options
        StartupMode startupMode = config.get(SCAN_STARTUP_MODE);
        validateStartupMode(startupMode, config);

        String specificOffsets = config.get(SCAN_STARTUP_SPECIFIC_OFFSETS);
        Long startupTimestampMillis = config.get(SCAN_STARTUP_TIMESTAMP_MILLIS);

        // Get schema inference and routing options
        boolean schemaInferEnabled = config.get(SCHEMA_INFER_ENABLED);
        String defaultDatabaseName = config.get(DATABASE_NAME);
        String defaultSchemaName = config.get(SCHEMA_NAME);
        String defaultTableName = config.get(TABLE_NAME);

        // Create deserialization schema
        CanalJsonDeserializationSchema deserializationSchema =
                new CanalJsonDeserializationSchema(
                        schemaInferEnabled,
                        defaultDatabaseName,
                        defaultSchemaName,
                        defaultTableName);

        return new KafkaDataSource(
                topic,
                kafkaProperties,
                groupId,
                startupMode,
                specificOffsets,
                startupTimestampMillis,
                deserializationSchema);
    }

    private void validateStartupMode(
            StartupMode startupMode,
            org.apache.flink.cdc.common.configuration.Configuration config) {
        switch (startupMode) {
            case EARLIEST_OFFSET:
            case LATEST_OFFSET:
            case GROUP_OFFSETS:
                break;
            case TIMESTAMP:
                if (config.get(SCAN_STARTUP_TIMESTAMP_MILLIS) == null) {
                    throw new ValidationException(
                            "'scan.startup.timestamp-millis' must be set when 'scan.startup.mode' is 'timestamp'.");
                }
                break;
            case SPECIFIC_OFFSETS:
                if (config.get(SCAN_STARTUP_SPECIFIC_OFFSETS) == null) {
                    throw new ValidationException(
                            "'scan.startup.specific-offsets' must be set when 'scan.startup.mode' is 'specific-offsets'.");
                }
                break;
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are "
                                        + "[earliest-offset, latest-offset, group-offsets, timestamp, specific-offsets], "
                                        + "but was: %s",
                                SCAN_STARTUP_MODE.key(), startupMode));
        }
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC);
        options.add(GROUP_ID);
        options.add(VALUE_FORMAT);
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_SPECIFIC_OFFSETS);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(DATABASE_NAME);
        options.add(SCHEMA_NAME);
        options.add(TABLE_NAME);
        options.add(SCHEMA_INFER_ENABLED);
        return options;
    }
}
