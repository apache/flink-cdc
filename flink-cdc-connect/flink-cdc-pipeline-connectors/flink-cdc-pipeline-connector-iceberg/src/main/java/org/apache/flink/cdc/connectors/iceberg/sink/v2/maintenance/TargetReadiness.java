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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.cdc.common.utils.Preconditions.checkArgument;

/** Validates each ready table once and routes changes without broadcasting to every target. */
final class TargetReadiness extends ProcessFunction<Tuple2<String, TableChange>, Void> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TargetReadiness.class);
    private final CatalogLoader catalogLoader;
    private final MaintenanceOptions options;
    private final Map<String, OutputTag<TableChange>> outputs = new HashMap<>();
    private transient Catalog catalog;
    private transient Set<String> ready;
    private transient Set<String> waiting;
    private transient Map<UUID, String> identifiersByUuid;

    TargetReadiness(CatalogLoader catalogLoader, MaintenanceOptions options) {
        this.catalogLoader = catalogLoader.clone();
        this.options = options;
        options.tables()
                .forEach(table -> outputs.put(table.identifier(), outputTag(table.identifier())));
    }

    static OutputTag<TableChange> outputTag(String identifier) {
        return new OutputTag<>(
                "maintenance-target-" + identifier, TypeInformation.of(TableChange.class));
    }

    @Override
    public void processElement(
            Tuple2<String, TableChange> value, Context context, Collector<Void> out) {
        checkArgument(outputs.containsKey(value.f0), "Unknown maintenance target: %s", value.f0);
        if (catalog == null) {
            catalog = catalogLoader.loadCatalog();
            ready = new HashSet<>();
            waiting = new HashSet<>();
            identifiersByUuid = new HashMap<>();
        }
        if (!ready.contains(value.f0)) {
            Table table;
            try {
                table = catalog.loadTable(TableIdentifier.parse(value.f0));
            } catch (NoSuchTableException e) {
                if (waiting.add(value.f0)) {
                    LOG.info("Waiting for CDC to create maintenance target {}", value.f0);
                }
                return;
            }
            checkArgument(
                    table instanceof BaseTable,
                    "CDC maintenance requires a catalog returning BaseTable: %s",
                    value.f0);
            String previous = identifiersByUuid.putIfAbsent(table.uuid(), value.f0);
            checkArgument(
                    previous == null || previous.equals(value.f0),
                    "Maintenance targets %s and %s refer to the same Iceberg table.",
                    previous,
                    value.f0);
            checkArgument(
                    !options.get(MaintenanceOptions.REWRITE_ENABLED)
                            || !TableUtil.supportsRowLineage(table),
                    "Flink does not support compaction on row lineage enabled tables (V3+): %s",
                    table.name());
            checkArgument(
                    !(options.get(MaintenanceOptions.EXPIRE_ENABLED)
                                    || options.get(MaintenanceOptions.ORPHAN_ENABLED))
                            || table.io() instanceof SupportsBulkOperations,
                    "Maintenance FileIO must support bulk deletion: %s",
                    table.name());
            checkArgument(
                    !options.get(MaintenanceOptions.ORPHAN_ENABLED)
                            || table.io() instanceof SupportsPrefixOperations,
                    "Orphan cleanup FileIO must support prefix listing: %s",
                    table.name());
            ready.add(value.f0);
            waiting.remove(value.f0);
            LOG.info("CDC target {} exists; starting maintenance", table.name());
        }
        context.output(outputs.get(value.f0), value.f1);
    }

    @Override
    public void close() throws Exception {
        if (catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
        catalog = null;
    }
}
