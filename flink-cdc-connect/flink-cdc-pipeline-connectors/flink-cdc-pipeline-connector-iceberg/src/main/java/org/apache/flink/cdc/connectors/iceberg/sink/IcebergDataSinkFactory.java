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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.OptionUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.apache.iceberg.CatalogProperties;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES;
import static org.apache.flink.cdc.connectors.iceberg.sink.IcebergDataSinkOptions.PREFIX_TABLE_PROPERTIES;

/** A {@link DataSinkFactory} for Apache Iceberg. */
public class IcebergDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "iceberg";

    // Hard code to disable catalog cache as we always need latest schema after schema change.
    private static final Map<String, String> FIXED_CATALOG_OPTIONS =
            ImmutableMap.of(CatalogProperties.CACHE_ENABLED, "false");

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PREFIX_TABLE_PROPERTIES, PREFIX_CATALOG_PROPERTIES);

        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        OptionUtils.printOptions(IDENTIFIER, allOptions);

        Map<String, String> catalogOptions = new HashMap<>();
        Map<String, String> tableOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PREFIX_TABLE_PROPERTIES)) {
                        tableOptions.put(key.substring(PREFIX_TABLE_PROPERTIES.length()), value);
                    } else if (key.startsWith(IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
                        catalogOptions.put(
                                key.substring(
                                        IcebergDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
                                value);
                    }
                });
        catalogOptions.putAll(FIXED_CATALOG_OPTIONS);
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        String schemaOperatorUid =
                context.getPipelineConfiguration()
                        .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        CompactionOptions compactionOptions =
                getCompactionStrategy(context.getFactoryConfiguration());

        return new IcebergDataSink(
                catalogOptions,
                tableOptions,
                new HashMap<>(),
                zoneId,
                schemaOperatorUid,
                compactionOptions);
    }

    private CompactionOptions getCompactionStrategy(Configuration configuration) {
        return CompactionOptions.builder()
                .enabled(configuration.get(IcebergDataSinkOptions.SINK_COMPACTION_ENABLED))
                .commitInterval(
                        configuration.get(IcebergDataSinkOptions.SINK_COMPACTION_COMMIT_INTERVAL))
                .parallelism(configuration.get(IcebergDataSinkOptions.SINK_COMPACTION_PARALLELISM))
                .build();
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
        options.add(IcebergDataSinkOptions.TYPE);
        options.add(IcebergDataSinkOptions.WAREHOUSE);
        options.add(IcebergDataSinkOptions.PARTITION_KEY);
        options.add(IcebergDataSinkOptions.SINK_COMPACTION_ENABLED);
        options.add(IcebergDataSinkOptions.SINK_COMPACTION_COMMIT_INTERVAL);
        return options;
    }
}
