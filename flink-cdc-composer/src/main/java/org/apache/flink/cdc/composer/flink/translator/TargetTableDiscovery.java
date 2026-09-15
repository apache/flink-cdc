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

package org.apache.flink.cdc.composer.flink.translator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.route.TableIdRouter;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.SupportsTargetTableDiscovery;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.SupportsTableDiscovery;
import org.apache.flink.cdc.composer.definition.PipelineDef;

import java.util.List;
import java.util.stream.Collectors;

/** Resolves submission-time target tables using the same routing as schema operators. */
@Internal
public final class TargetTableDiscovery {
    private TargetTableDiscovery() {}

    public static void initialize(PipelineDef pipeline, DataSource source, DataSink sink) {
        if (sink instanceof SupportsTargetTableDiscovery) {
            ((SupportsTargetTableDiscovery) sink)
                    .discoverTargetTables(() -> discover(pipeline, source));
        }
    }

    private static List<TableId> discover(PipelineDef pipeline, DataSource source) {
        if (!(source instanceof SupportsTableDiscovery)) {
            throw new IllegalArgumentException(
                    "Source '"
                            + pipeline.getSource().getType()
                            + "' does not support submission-time table discovery. "
                            + "Configure the sink's target tables explicitly "
                            + "(sink.maintenance.tables for Iceberg).");
        }
        List<TableId> capturedTables;
        try {
            capturedTables = ((SupportsTableDiscovery) source).listCapturedTables();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                    "Cannot discover captured tables from source '"
                            + pipeline.getSource().getType()
                            + "' before submission.",
                    e);
        }
        TableIdRouter router =
                new TableIdRouter(
                        pipeline.getRoute().stream()
                                .map(
                                        route ->
                                                new RouteRule(
                                                        route.getSourceTable(),
                                                        route.getSinkTable(),
                                                        route.getReplaceSymbol().orElse(null)))
                                .collect(Collectors.toList()),
                        pipeline.getRouteMode());
        return capturedTables.stream()
                .flatMap(table -> router.route(table).stream())
                .collect(Collectors.toList());
    }
}
