/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.sink.DataSink;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.JDBC_URL;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.LOAD_URL;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.PASSWORD;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_BATCH_FLUSH_INTERVAL;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_BATCH_MAX_SIZE;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_CONNECT_TIMEOUT;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_IO_THREAD_COUNT;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_LABEL_PREFIX;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_PROPERTIES_PREFIX;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_SCAN_FREQUENCY;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_CREATE_NUM_BUCKETS;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.TABLE_SCHEMA_CHANGE_TIMEOUT;
import static com.ververica.cdc.connectors.starrocks.sink.StarRocksDataSinkOptions.USERNAME;

/** A {@link DataSinkFactory} to create {@link StarRocksDataSink}. */
public class StarRocksDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "starrocks";

    @Override
    public DataSink createDataSink(Context context) {
        StarRocksSinkOptions sinkOptions = buildSinkConnectorOptions(context.getConfiguration());
        TableConfig tableConfig = TableConfig.from(context.getConfiguration());
        SchemaChangeConfig schemaChangeConfig = SchemaChangeConfig.from(context.getConfiguration());
        return new StarRocksDataSink(sinkOptions, tableConfig, schemaChangeConfig);
    }

    private StarRocksSinkOptions buildSinkConnectorOptions(Configuration cdcConfig) {
        org.apache.flink.configuration.Configuration sinkConfig =
                new org.apache.flink.configuration.Configuration();
        // sink configurations from users
        sinkConfig.set(StarRocksSinkOptions.JDBC_URL, cdcConfig.get(JDBC_URL));
        sinkConfig.set(StarRocksSinkOptions.LOAD_URL, cdcConfig.get(LOAD_URL));
        sinkConfig.set(StarRocksSinkOptions.USERNAME, cdcConfig.get(USERNAME));
        sinkConfig.set(StarRocksSinkOptions.PASSWORD, cdcConfig.get(PASSWORD));
        sinkConfig.set(StarRocksSinkOptions.SINK_LABEL_PREFIX, cdcConfig.get(SINK_LABEL_PREFIX));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_CONNECT_TIMEOUT, cdcConfig.get(SINK_CONNECT_TIMEOUT));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_WAIT_FOR_CONTINUE_TIMEOUT,
                cdcConfig.get(SINK_WAIT_FOR_CONTINUE_TIMEOUT));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_BATCH_MAX_SIZE, cdcConfig.get(SINK_BATCH_MAX_SIZE));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_BATCH_FLUSH_INTERVAL,
                cdcConfig.get(SINK_BATCH_FLUSH_INTERVAL));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_SCAN_FREQUENCY, cdcConfig.get(SINK_SCAN_FREQUENCY));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_IO_THREAD_COUNT, cdcConfig.get(SINK_IO_THREAD_COUNT));
        sinkConfig.set(
                StarRocksSinkOptions.SINK_METRIC_HISTOGRAM_WINDOW_SIZE,
                cdcConfig.get(SINK_METRIC_HISTOGRAM_WINDOW_SIZE));
        // specified sink configurations for cdc scenario
        sinkConfig.set(StarRocksSinkOptions.DATABASE_NAME, "*");
        sinkConfig.set(StarRocksSinkOptions.TABLE_NAME, "*");
        sinkConfig.set(StarRocksSinkOptions.SINK_USE_NEW_SINK_API, true);

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
        requiredOptions.add(JDBC_URL);
        requiredOptions.add(LOAD_URL);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(SINK_LABEL_PREFIX);
        optionalOptions.add(SINK_CONNECT_TIMEOUT);
        optionalOptions.add(SINK_WAIT_FOR_CONTINUE_TIMEOUT);
        optionalOptions.add(SINK_BATCH_MAX_SIZE);
        optionalOptions.add(SINK_BATCH_FLUSH_INTERVAL);
        optionalOptions.add(SINK_SCAN_FREQUENCY);
        optionalOptions.add(SINK_IO_THREAD_COUNT);
        optionalOptions.add(SINK_AT_LEAST_ONCE_USE_TRANSACTION_LOAD);
        optionalOptions.add(SINK_METRIC_HISTOGRAM_WINDOW_SIZE);
        optionalOptions.add(TABLE_CREATE_NUM_BUCKETS);
        optionalOptions.add(TABLE_SCHEMA_CHANGE_TIMEOUT);
        return optionalOptions;
    }
}
