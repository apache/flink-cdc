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

package org.apache.flink.cdc.connectors.hudi.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.hudi.sink.v2.HudiSink;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.hudi.sink.HudiDataSinkOptions.PREFIX_TABLE_PROPERTIES;

/**
 * A {@link DataSink} for Apache Hudi that provides the main entry point for the Flink CDC
 * framework.
 */
public class HudiDataSink implements DataSink, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HudiDataSink.class);

    private final Configuration config;

    private final String schemaOperatorUid;

    public HudiDataSink(Configuration config, String schemaOperatorUid) {
        LOG.info("Creating HudiDataSink with universal configuration {}", config);
        this.config = normalizeOptions(config);
        this.schemaOperatorUid = schemaOperatorUid;
    }

    /** Provides the core sink implementation that handles the data flow of events. */
    @Override
    public EventSinkProvider getEventSinkProvider() {
        LOG.info("Creating HudiDataSinkProvider with universal configuration {}", config);
        // For CDC pipelines, we don't have a pre-configured schema since tables are created
        // dynamically
        // Instead, we use a multi-table sink that handles schema discovery and table creation

        // Convert CDC configuration to Flink configuration for HoodieSink
        org.apache.flink.configuration.Configuration flinkConfig = toFlinkConfig(config);

        // Create the HudiSink with multi-table support via wrapper pattern
        // Use empty RowType since tables are created dynamically
        HudiSink hudiSink = new HudiSink(flinkConfig, schemaOperatorUid, ZoneId.systemDefault());

        return FlinkSinkProvider.of(hudiSink);
    }

    /**
     * Provides the metadata applier. In our design, this has a passive role (e.g., logging), as
     * transactional metadata operations are handled by the HudiCommitter.
     */
    @Override
    public MetadataApplier getMetadataApplier() {
        return new HudiMetadataApplier(config);
    }

    /**
     * Converts a {@link org.apache.flink.cdc.common.configuration.Configuration} to a {@link
     * org.apache.flink.configuration.Configuration}.
     *
     * @param cdcConfig The input CDC configuration.
     * @return A new Flink configuration containing the same key-value pairs.
     */
    private static org.apache.flink.configuration.Configuration toFlinkConfig(
            Configuration cdcConfig) {
        final org.apache.flink.configuration.Configuration flinkConfig =
                new org.apache.flink.configuration.Configuration();
        if (cdcConfig != null) {
            cdcConfig.toMap().forEach(flinkConfig::setString);
        }
        // always enable schema evolution
        flinkConfig.setString(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key(), "true");
        return flinkConfig;
    }

    private static Configuration normalizeOptions(Configuration config) {
        Map<String, String> options = new HashMap<>();
        config.toMap()
                .forEach(
                        (key, val) -> {
                            if (key.startsWith(PREFIX_TABLE_PROPERTIES)) {
                                options.put(key.substring(PREFIX_TABLE_PROPERTIES.length()), val);
                            } else {
                                options.put(key, val);
                            }
                        });
        return Configuration.fromMap(options);
    }
}
