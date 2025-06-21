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

package org.apache.flink.cdc.connectors.doris.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.doris.sink.DorisDataSink;
import org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.AUTO_REDIRECT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.BENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.CHARSET_ENCODING;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.FENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.JDBC_URL;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_BUFFER_COUNT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_BUFFER_FLUSH_MAX_BYTES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_BUFFER_SIZE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_CHECK_INTERVAL;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_2PC;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_BATCH_MODE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_FLUSH_QUEUE_SIZE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_IGNORE_UPDATE_BEFORE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_LABEL_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_MAX_RETRIES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_USE_CACHE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.STREAM_LOAD_PROP_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.USERNAME;

/** A dummy {@link DataSinkFactory} to create {@link DorisDataSink}. */
@Internal
public class DorisDataSinkFactory implements DataSinkFactory {
    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(
                        TABLE_CREATE_PROPERTIES_PREFIX,
                        STREAM_LOAD_PROP_PREFIX,
                        TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX);

        Configuration config = context.getFactoryConfiguration();
        DorisOptions.Builder optionsBuilder = DorisOptions.builder();
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        config.getOptional(FENODES).ifPresent(optionsBuilder::setFenodes);
        config.getOptional(BENODES).ifPresent(optionsBuilder::setBenodes);
        config.getOptional(USERNAME).ifPresent(optionsBuilder::setUsername);
        config.getOptional(PASSWORD).ifPresent(optionsBuilder::setPassword);
        config.getOptional(JDBC_URL).ifPresent(optionsBuilder::setJdbcUrl);
        config.getOptional(AUTO_REDIRECT).ifPresent(optionsBuilder::setAutoRedirect);

        config.getOptional(SINK_CHECK_INTERVAL).ifPresent(executionBuilder::setCheckInterval);
        config.getOptional(SINK_MAX_RETRIES).ifPresent(executionBuilder::setMaxRetries);
        config.getOptional(SINK_ENABLE_DELETE).ifPresent(executionBuilder::setDeletable);
        config.getOptional(SINK_LABEL_PREFIX).ifPresent(executionBuilder::setLabelPrefix);
        config.getOptional(SINK_BUFFER_SIZE).ifPresent(executionBuilder::setBufferSize);
        config.getOptional(SINK_BUFFER_COUNT).ifPresent(executionBuilder::setBufferCount);
        config.getOptional(SINK_BUFFER_FLUSH_MAX_ROWS)
                .ifPresent(executionBuilder::setBufferFlushMaxRows);
        config.getOptional(SINK_BUFFER_FLUSH_MAX_BYTES)
                .ifPresent(executionBuilder::setBufferFlushMaxBytes);
        config.getOptional(SINK_FLUSH_QUEUE_SIZE).ifPresent(executionBuilder::setFlushQueueSize);
        config.getOptional(SINK_IGNORE_UPDATE_BEFORE)
                .ifPresent(executionBuilder::setIgnoreUpdateBefore);
        config.getOptional(SINK_USE_CACHE).ifPresent(executionBuilder::setUseCache);
        config.getOptional(SINK_BUFFER_FLUSH_INTERVAL)
                .ifPresent(v -> executionBuilder.setBufferFlushIntervalMs(v.toMillis()));
        config.getOptional(SINK_ENABLE_2PC)
                .ifPresent(
                        b -> {
                            if (b) {
                                executionBuilder.enable2PC();
                            } else {
                                executionBuilder.disable2PC();
                            }
                        });
        // default batch mode
        executionBuilder.setBatchMode(config.get(SINK_ENABLE_BATCH_MODE));

        // set streamload properties
        Properties properties = DorisExecutionOptions.defaultsProperties();
        Map<String, String> streamLoadProp =
                DorisDataSinkOptions.getPropertiesByPrefix(config, STREAM_LOAD_PROP_PREFIX);
        properties.putAll(streamLoadProp);
        executionBuilder.setStreamLoadProp(properties);

        return new DorisDataSink(
                optionsBuilder.build(),
                DorisReadOptions.builder().build(),
                executionBuilder.build(),
                config,
                ZoneId.of(
                        context.getPipelineConfiguration()
                                .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE)));
    }

    @Override
    public String identifier() {
        return "doris";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FENODES);
        options.add(USERNAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BENODES);
        options.add(JDBC_URL);
        options.add(PASSWORD);
        options.add(AUTO_REDIRECT);
        options.add(CHARSET_ENCODING);

        options.add(SINK_CHECK_INTERVAL);
        options.add(SINK_ENABLE_2PC);
        options.add(SINK_MAX_RETRIES);
        options.add(SINK_ENABLE_DELETE);
        options.add(SINK_LABEL_PREFIX);
        options.add(SINK_BUFFER_SIZE);
        options.add(SINK_BUFFER_COUNT);

        options.add(SINK_ENABLE_BATCH_MODE);
        options.add(SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(SINK_BUFFER_FLUSH_MAX_BYTES);
        options.add(SINK_FLUSH_QUEUE_SIZE);
        options.add(SINK_BUFFER_FLUSH_INTERVAL);
        options.add(SINK_IGNORE_UPDATE_BEFORE);
        options.add(SINK_USE_CACHE);

        return options;
    }
}
