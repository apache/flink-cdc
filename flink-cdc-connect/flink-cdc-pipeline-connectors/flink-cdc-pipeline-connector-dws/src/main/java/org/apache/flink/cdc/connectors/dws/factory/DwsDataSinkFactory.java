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

package org.apache.flink.cdc.connectors.dws.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.dws.sink.DwsDataSink;

import com.huaweicloud.dws.client.DwsConfig;
import com.huaweicloud.dws.client.config.DwsClientConfigs;
import com.huaweicloud.dws.client.model.PartitionPolicy;
import com.huaweicloud.dws.client.model.WriteMode;
import com.huaweicloud.dws.connectors.flink.config.DwsConnectionOptions;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.AUTO_BATCH_FLUSH_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.AUTO_FLUSH_MAX_INTERVAL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CASE_SENSITIVE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_IDLE_MS;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_LIFETIME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_USE_COUNT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_MAX_USE_TIME_SECONDS;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_MAX_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_MIN_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_MONITOR_PERIOD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_NAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_POOL_TIMEOUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_SOCKET_TIMEOUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.CONNECTION_TIME_OUT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DATABASE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DISTRIBUTION_KEY;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.DRIVER;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.ENABLE_AUTO_FLUSH;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.ENABLE_DN_PARTITION;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.LOG_SWITCH;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.NEED_CONNECTION_POOL_MONITOR;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.PIPELINE_LOCAL_TIME_ZONE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SCHEMA;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_BUFFER_FLUSH_MAX_SIZE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_TABLE_CREATE;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_ENABLE_UPSERT;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.SINK_PARALLELISM;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.TABLE_CREATE_PROPERTIES;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.TABLE_NAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.URL;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.USERNAME;
import static org.apache.flink.cdc.connectors.dws.sink.DwsDataSinkOptions.WRITE_MODE;

/** A {@link DataSinkFactory} to create {@link DwsDataSink}. */
@Internal
public class DwsDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "dws";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context).validate();

        Configuration config = context.getFactoryConfiguration();
        ZoneId zoneId = resolveZoneId(config, context.getPipelineConfiguration());
        Duration autoFlushMaxInterval = config.get(AUTO_FLUSH_MAX_INTERVAL);

        return new DwsDataSink(
                buildConnectorOptions(config),
                zoneId,
                config.get(CASE_SENSITIVE),
                config.get(SCHEMA),
                config.get(AUTO_BATCH_FLUSH_SIZE),
                autoFlushMaxInterval,
                config.get(ENABLE_AUTO_FLUSH),
                parseWriteMode(config.get(WRITE_MODE)),
                config.get(ENABLE_DN_PARTITION),
                config.getOptional(DISTRIBUTION_KEY).orElse(null));
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(URL);
        requiredOptions.add(USERNAME);
        requiredOptions.add(PASSWORD);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(AUTO_BATCH_FLUSH_SIZE);
        optionalOptions.add(AUTO_FLUSH_MAX_INTERVAL);
        optionalOptions.add(LOG_SWITCH);
        optionalOptions.add(DATABASE);
        optionalOptions.add(TABLE_NAME);
        optionalOptions.add(DRIVER);
        optionalOptions.add(PIPELINE_LOCAL_TIME_ZONE);
        optionalOptions.add(CONNECTION_SIZE);
        optionalOptions.add(SCHEMA);
        optionalOptions.add(ENABLE_AUTO_FLUSH);
        optionalOptions.add(ENABLE_DN_PARTITION);
        optionalOptions.add(DISTRIBUTION_KEY);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        optionalOptions.add(SINK_BUFFER_FLUSH_MAX_SIZE);
        optionalOptions.add(SINK_BUFFER_FLUSH_INTERVAL);
        optionalOptions.add(SINK_MAX_RETRIES);
        optionalOptions.add(SINK_ENABLE_DELETE);
        optionalOptions.add(SINK_ENABLE_UPSERT);
        optionalOptions.add(CONNECTION_POOL_MAX_SIZE);
        optionalOptions.add(CONNECTION_POOL_MIN_SIZE);
        optionalOptions.add(CONNECTION_MAX_LIFETIME);
        optionalOptions.add(SINK_ENABLE_TABLE_CREATE);
        optionalOptions.add(TABLE_CREATE_PROPERTIES);
        optionalOptions.add(CONNECTION_MAX_USE_TIME_SECONDS);
        optionalOptions.add(CONNECTION_MAX_IDLE_MS);
        optionalOptions.add(CONNECTION_TIME_OUT);
        optionalOptions.add(CONNECTION_POOL_NAME);
        optionalOptions.add(CONNECTION_POOL_SIZE);
        optionalOptions.add(CONNECTION_POOL_TIMEOUT);
        optionalOptions.add(CONNECTION_SOCKET_TIMEOUT);
        optionalOptions.add(CONNECTION_MAX_USE_COUNT);
        optionalOptions.add(NEED_CONNECTION_POOL_MONITOR);
        optionalOptions.add(CONNECTION_POOL_MONITOR_PERIOD);
        optionalOptions.add(SINK_PARALLELISM);
        optionalOptions.add(CASE_SENSITIVE);
        optionalOptions.add(WRITE_MODE);
        return optionalOptions;
    }

    private static DwsConnectionOptions buildConnectorOptions(Configuration config) {
        DwsConfig dwsConfig =
                DwsConfig.builder()
                        .with(
                                DwsClientConfigs.WRITE_PARTITION_POLICY,
                                config.get(ENABLE_DN_PARTITION)
                                        ? PartitionPolicy.DN
                                        : PartitionPolicy.DYNAMIC)
                        .build();

        DwsConnectionOptions.Builder builder =
                DwsConnectionOptions.builder()
                        .withUrl(config.get(URL))
                        .withUsername(config.get(USERNAME))
                        .withPassword(config.get(PASSWORD))
                        .withDriver(config.get(DRIVER))
                        .withLogSwitch(config.get(LOG_SWITCH))
                        .withConnectionSize(config.get(CONNECTION_SIZE))
                        .withConnectionMaxUseTimeSeconds(
                                config.get(CONNECTION_MAX_USE_TIME_SECONDS))
                        .withConnectionMaxIdleMs(config.get(CONNECTION_MAX_IDLE_MS))
                        .withConnectionTimeOut(config.get(CONNECTION_TIME_OUT))
                        .withConnectionPoolName(config.get(CONNECTION_POOL_NAME))
                        .withConnectionPoolSize(config.get(CONNECTION_POOL_SIZE))
                        .withConnectionPoolTimeout(config.get(CONNECTION_POOL_TIMEOUT))
                        .withConnectionSocketTimeout(config.get(CONNECTION_SOCKET_TIMEOUT))
                        .withConnectionMaxUseCount(config.get(CONNECTION_MAX_USE_COUNT))
                        .withNeedConnectionPoolMonitor(config.get(NEED_CONNECTION_POOL_MONITOR))
                        .withConnectionPoolMonitorPeriod(config.get(CONNECTION_POOL_MONITOR_PERIOD))
                        .withConfig(dwsConfig);

        config.getOptional(SINK_PARALLELISM).ifPresent(builder::withParallelism);
        config.getOptional(TABLE_NAME).ifPresent(builder::withTableName);
        return builder.build();
    }

    private static ZoneId resolveZoneId(
            Configuration factoryConfiguration, Configuration pipelineConfiguration) {
        String zone =
                factoryConfiguration.contains(PIPELINE_LOCAL_TIME_ZONE)
                        ? factoryConfiguration.get(PIPELINE_LOCAL_TIME_ZONE)
                        : pipelineConfiguration.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE);
        return PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }

    private static WriteMode parseWriteMode(String writeMode) {
        if (writeMode == null) {
            return null;
        }

        try {
            return WriteMode.valueOf(writeMode.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Unsupported write-mode '%s'. Available values: %s",
                            writeMode, Arrays.toString(WriteMode.values())),
                    e);
        }
    }
}
