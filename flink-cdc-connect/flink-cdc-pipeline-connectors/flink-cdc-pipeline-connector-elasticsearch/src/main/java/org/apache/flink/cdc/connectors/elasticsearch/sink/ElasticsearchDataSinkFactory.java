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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.flink.cdc.connectors.elasticsearch.v2.NetworkConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.HOSTS;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_BATCH_SIZE;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_BATCH_SIZE_IN_BYTES;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_BUFFERED_REQUESTS;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_IN_FLIGHT_REQUESTS;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_RECORD_SIZE_IN_BYTES;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.MAX_TIME_IN_BUFFER_MS;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.SHARDING_SUFFIX_KEY;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.SHARDING_SUFFIX_SEPARATOR;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.elasticsearch.sink.ElasticsearchDataSinkOptions.VERSION;

/** Factory for creating {@link ElasticsearchDataSink}. */
public class ElasticsearchDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "elasticsearch";
    private static final String ES_INDEX_ILLEGAL_CHARS = "\\/*?\"<>| ,#";

    @Override
    public DataSink createDataSink(Context context) {
        // Validate the configuration
        FactoryHelper.createFactoryHelper(this, context).validate();

        // Get the configuration directly from the context
        Configuration configuration =
                Configuration.fromMap(context.getFactoryConfiguration().toMap());

        // Validate required options
        validateRequiredOptions(configuration);

        ZoneId zoneId = determineZoneId(context);
        ElasticsearchSinkOptions sinkOptions = buildSinkConnectorOptions(configuration);
        return new ElasticsearchDataSink(sinkOptions, zoneId);
    }

    private ZoneId determineZoneId(Context context) {
        String configuredZone =
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE);
        String defaultZone = PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue();

        return Objects.equals(configuredZone, defaultZone)
                ? ZoneId.systemDefault()
                : ZoneId.of(configuredZone);
    }

    private ElasticsearchSinkOptions buildSinkConnectorOptions(Configuration cdcConfig) {
        List<HttpHost> hosts = parseHosts(cdcConfig.get(HOSTS));
        String username = cdcConfig.get(USERNAME);
        String password = cdcConfig.get(PASSWORD);
        int version = cdcConfig.get(VERSION);
        Map<TableId, String> shardingMaps = new HashMap<>();
        String shardingKey = cdcConfig.get(SHARDING_SUFFIX_KEY);
        String shardingSeparator = cdcConfig.get(SHARDING_SUFFIX_SEPARATOR);
        if (!shardingKey.isEmpty()) {
            for (String tables : shardingKey.split(";")) {
                String[] splits = tables.split(":");
                if (splits.length == 2) {
                    TableId tableId = TableId.parse(splits[0]);
                    shardingMaps.put(tableId, splits[1].trim());
                } else {
                    throw new IllegalArgumentException(
                            String.format(
                                    "%s is malformed, please refer to the documents",
                                    SHARDING_SUFFIX_KEY.key()));
                }
            }
        }
        validateShardingSeparator(shardingSeparator);

        NetworkConfig networkConfig =
                new NetworkConfig(hosts, username, password, null, null, null);
        return new ElasticsearchSinkOptions(
                cdcConfig.get(MAX_BATCH_SIZE),
                cdcConfig.get(MAX_IN_FLIGHT_REQUESTS),
                cdcConfig.get(MAX_BUFFERED_REQUESTS),
                cdcConfig.get(MAX_BATCH_SIZE_IN_BYTES),
                cdcConfig.get(MAX_TIME_IN_BUFFER_MS),
                cdcConfig.get(MAX_RECORD_SIZE_IN_BYTES),
                networkConfig,
                version,
                username,
                password,
                shardingMaps,
                shardingSeparator);
    }

    private List<HttpHost> parseHosts(String hostsStr) {
        return Arrays.stream(hostsStr.split(","))
                .map(HttpHost::create)
                .collect(Collectors.toList());
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(HOSTS);
        requiredOptions.add(VERSION);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(MAX_BATCH_SIZE);
        optionalOptions.add(MAX_IN_FLIGHT_REQUESTS);
        optionalOptions.add(MAX_BUFFERED_REQUESTS);
        optionalOptions.add(MAX_BATCH_SIZE_IN_BYTES);
        optionalOptions.add(MAX_TIME_IN_BUFFER_MS);
        optionalOptions.add(MAX_RECORD_SIZE_IN_BYTES);
        optionalOptions.add(USERNAME);
        optionalOptions.add(PASSWORD);
        optionalOptions.add(SHARDING_SUFFIX_KEY);
        optionalOptions.add(SHARDING_SUFFIX_SEPARATOR);
        return optionalOptions;
    }

    private void validateRequiredOptions(Configuration configuration) {
        Set<ConfigOption<?>> missingOptions = new HashSet<>();
        for (ConfigOption<?> option : requiredOptions()) {
            if (!configuration.contains(option)) {
                missingOptions.add(option);
            }
        }
        if (!missingOptions.isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "One or more required options are missing.\n\n"
                                    + "Missing required options are:\n\n"
                                    + "%s",
                            missingOptions.stream()
                                    .map(ConfigOption::key)
                                    .collect(Collectors.joining("\n"))));
        }
    }

    private void validateShardingSeparator(String separator) {
        if (!separator.equals(separator.toLowerCase())) {
            throw new ValidationException(
                    String.format(
                            "%s is malformed, elasticsearch index only support lowercase.",
                            SHARDING_SUFFIX_SEPARATOR.key()));
        }

        for (char c : ES_INDEX_ILLEGAL_CHARS.toCharArray()) {
            if (separator.indexOf(c) != -1) {
                throw new ValidationException(
                        String.format(
                                "%s is malformed, elasticsearch index cannot include \\, /, *, ?, \", <, >, |, ` ` (space character), ,, #",
                                SHARDING_SUFFIX_SEPARATOR.key()));
            }
        }
    }
}
