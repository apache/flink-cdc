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

package org.apache.flink.cdc.connectors.starrocks.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.sink.DataSink;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.pipeline.PipelineOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** A {@link DataSinkFactory} to create {@link StarRocksDataSink}. */
public class StarRocksDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "starrocks";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(TABLE_CREATE_PROPERTIES_PREFIX, SINK_PROPERTIES_PREFIX);

        StarRocksSinkOptions sinkOptions =
                buildSinkConnectorOptions(context.getFactoryConfiguration());
        TableCreateConfig tableCreateConfig =
                TableCreateConfig.from(context.getFactoryConfiguration());
        SchemaChangeConfig schemaChangeConfig =
                SchemaChangeConfig.from(context.getFactoryConfiguration());
        String zoneStr = context.getPipelineConfiguration().get(PIPELINE_LOCAL_TIME_ZONE);
        ZoneId zoneId =
                PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zoneStr)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneStr);
        return new StarRocksDataSink(sinkOptions, tableCreateConfig, schemaChangeConfig, zoneId);
    }

    private StarRocksSinkOptions buildSinkConnectorOptions(Configuration cdcConfig) {
        org.apache.flink.configuration.Configuration sinkConfig =
                new org.apache.flink.configuration.Configuration();
        // required sink configurations
        sinkConfig.set(
                StarRocksSinkOptions.JDBC_URL, cdcConfig.get(StarRocksDataSinkOptions.JDBC_URL));
        sinkConfig.set(
                StarRocksSinkOptions.LOAD_URL, cdcConfig.get(StarRocksDataSinkOptions.LOAD_URL));
        sinkConfig.set(
                StarRocksSinkOptions.USERNAME, cdcConfig.get(StarRocksDataSinkOptions.USERNAME));
        sinkConfig.set(
                StarRocksSinkOptions.PASSWORD, cdcConfig.get(StarRocksDataSinkOptions.PASSWORD));
        // optional sink configurations
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_LABEL_PREFIX)
                .ifPresent(
                        config -> sinkConfig.set(StarRocksSinkOptions.SINK_LABEL_PREFIX, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_CONNECT_TIMEOUT)
                .ifPresent(
                        config ->
                                sinkConfig.set(StarRocksSinkOptions.SINK_CONNECT_TIMEOUT, config));

        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_SOCKET_TIMEOUT)
                .ifPresent(
                        config -> sinkConfig.set(StarRocksSinkOptions.SINK_SOCKET_TIMEOUT, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT)
                .ifPresent(
                        config ->
                                sinkConfig.set(
                                        StarRocksSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT,
                                        config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_BATCH_MAX_SIZE)
                .ifPresent(
                        config -> sinkConfig.set(StarRocksSinkOptions.SINK_BATCH_MAX_SIZE, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_BATCH_FLUSH_INTERVAL)
                .ifPresent(
                        config ->
                                sinkConfig.set(
                                        StarRocksSinkOptions.SINK_BATCH_FLUSH_INTERVAL, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_SCAN_FREQUENCY)
                .ifPresent(
                        config -> sinkConfig.set(StarRocksSinkOptions.SINK_SCAN_FREQUENCY, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_IO_THREAD_COUNT)
                .ifPresent(
                        config ->
                                sinkConfig.set(StarRocksSinkOptions.SINK_IO_THREAD_COUNT, config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD)
                .ifPresent(
                        config ->
                                sinkConfig.set(
                                        StarRocksSinkOptions
                                                .SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD,
                                        config));
        cdcConfig
                .getOptional(StarRocksDataSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE)
                .ifPresent(
                        config ->
                                sinkConfig.set(
                                        StarRocksSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE,
                                        config));
        // specified sink configurations for cdc scenario
        sinkConfig.set(StarRocksSinkOptions.DATABASE_NAME, "*");
        sinkConfig.set(StarRocksSinkOptions.TABLE_NAME, "*");
        sinkConfig.set(StarRocksSinkOptions.SINK_USE_NEW_SINK_API, true);
        // currently cdc framework only supports at-least-once
        sinkConfig.set(StarRocksSinkOptions.SINK_SEMANTIC, "at-least-once");

        Map<String, String> streamProperties =
                getPrefixConfigs(cdcConfig.toMap(), SINK_PROPERTIES_PREFIX);
        // force to use json format for stream load to simplify the configuration,
        // such as there is no need to reconfigure the "columns" property after
        // schema change. csv format can be supported in the future if needed
        streamProperties.put("sink.properties.format", "json");
        streamProperties.put("sink.properties.strip_outer_array", "true");
        streamProperties.put("sink.properties.ignore_json_size", "true");

        return new StarRocksSinkOptions(sinkConfig, streamProperties);
    }

    private Map<String, String> getPrefixConfigs(Map<String, String> config, String prefix) {
        return config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(StarRocksDataSinkOptions.JDBC_URL);
        requiredOptions.add(StarRocksDataSinkOptions.LOAD_URL);
        requiredOptions.add(StarRocksDataSinkOptions.USERNAME);
        requiredOptions.add(StarRocksDataSinkOptions.PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(StarRocksDataSinkOptions.SINK_LABEL_PREFIX);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_CONNECT_TIMEOUT);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_SOCKET_TIMEOUT);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_BATCH_MAX_SIZE);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_BATCH_FLUSH_INTERVAL);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_SCAN_FREQUENCY);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_IO_THREAD_COUNT);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD);
        optionalOptions.add(StarRocksDataSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE);
        optionalOptions.add(StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS);
        optionalOptions.add(StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT);
        return optionalOptions;
    }
}
