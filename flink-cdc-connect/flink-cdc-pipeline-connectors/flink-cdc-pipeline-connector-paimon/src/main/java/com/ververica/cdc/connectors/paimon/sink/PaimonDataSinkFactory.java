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

package com.ververica.cdc.connectors.paimon.sink;

import com.ververica.cdc.common.configuration.ConfigOption;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.factories.DataSinkFactory;
import com.ververica.cdc.common.pipeline.PipelineOptions;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.utils.Preconditions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.options.Options;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** A {@link DataSinkFactory} to create {@link PaimonDataSink}. */
public class PaimonDataSinkFactory implements DataSinkFactory {

    public static final String IDENTIFIER = "paimon";

    @Override
    public DataSink createDataSink(Context context) {
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        Map<String, String> catalogOptions = new HashMap<>();
        Map<String, String> tableOptions = new HashMap<>();
        allOptions.forEach(
                (key, value) -> {
                    if (key.startsWith(PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES)) {
                        tableOptions.put(
                                key.substring(
                                        PaimonDataSinkOptions.PREFIX_TABLE_PROPERTIES.length()),
                                value);
                    } else if (key.startsWith(PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES)) {
                        catalogOptions.put(
                                key.substring(
                                        PaimonDataSinkOptions.PREFIX_CATALOG_PROPERTIES.length()),
                                value);
                    }
                });
        Options options = Options.fromMap(catalogOptions);
        Catalog catalog = FlinkCatalogFactory.createPaimonCatalog(options);
        Preconditions.checkNotNull(catalog.listDatabases(), "catalog option of Paimon is invalid.");
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
        for (String tables : partitionKey.split(";")) {
            String[] splits = tables.split(":");
            if (splits.length == 2) {
                TableId tableId = TableId.parse(splits[0]);
                List<String> partitions = Arrays.asList(splits[1].split(","));
                partitionMaps.put(tableId, partitions);
            }
        }
        return new PaimonDataSink(options, tableOptions, zoneId, commitUser, partitionMaps);
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
        options.add(PaimonDataSinkOptions.URI);
        options.add(PaimonDataSinkOptions.WAREHOUSE);
        options.add(PaimonDataSinkOptions.COMMIT_USER);
        return options;
    }
}
