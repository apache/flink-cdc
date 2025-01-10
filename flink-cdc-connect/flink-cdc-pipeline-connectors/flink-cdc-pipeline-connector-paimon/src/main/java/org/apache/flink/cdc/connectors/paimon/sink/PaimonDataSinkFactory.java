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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSinkFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordEventSerializer;
import org.apache.flink.cdc.connectors.paimon.sink.v2.PaimonRecordSerializer;

import org.apache.paimon.options.Options;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES;
import static org.apache.flink.cdc.connectors.paimon.sink.PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES;

/** A {@link DataSinkFactory} to create {@link PaimonDataSink}. */
public class PaimonDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "paimon";

    @Override
    public DataSink createDataSink(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PREFIX_TABLE_PROPERTIES, PREFIX_CATALOG_PROPERTIES);

        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        Map<String, String> catalogOptions = new HashMap<>();
        Map<String, String> tableOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PREFIX_TABLE_PROPERTIES)) {
                        tableOptions.put(key.substring(PREFIX_TABLE_PROPERTIES.length()), value);
                    } else if (key.startsWith(PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
                        catalogOptions.put(
                                key.substring(
                                        PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
                                value);
                    }
                });
        Options options = Options.fromMap(catalogOptions);
        // Avoid using previous table schema.
        options.setString("cache-enabled", "false");
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        String commitUser =
                context.getFactoryConfiguration().get(PaimonDataSinkOptions.COMMIT_USER);
        String partitionKey =
                context.getFactoryConfiguration().get(PaimonDataSinkOptions.PARTITION_KEY);
        Map<TableId, List<String>> partitionMaps = new HashMap<>();
        if (!partitionKey.isEmpty()) {
            for (String tables : partitionKey.split(";")) {
                String[] splits = tables.split(":");
                if (splits.length == 2) {
                    TableId tableId = TableId.parse(splits[0]);
                    List<String> partitions = Arrays.asList(splits[1].split(","));
                    partitionMaps.put(tableId, partitions);
                } else {
                    throw new IllegalArgumentException(
                            PaimonDataSinkOptions.PARTITION_KEY.key()
                                    + " is malformed, please refer to the documents");
                }
            }
        }
        PaimonRecordSerializer<Event> serializer = new PaimonRecordEventSerializer(zoneId);
        String schemaOperatorUid =
                context.getPipelineConfiguration()
                        .get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID);
        return new PaimonDataSink(
                options,
                tableOptions,
                commitUser,
                partitionMaps,
                serializer,
                zoneId,
                schemaOperatorUid);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PaimonDataSinkOptions.METASTORE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PaimonDataSinkOptions.WAREHOUSE);
        options.add(PaimonDataSinkOptions.URI);
        options.add(PaimonDataSinkOptions.COMMIT_USER);
        options.add(PaimonDataSinkOptions.PARTITION_KEY);
        return options;
    }
}
