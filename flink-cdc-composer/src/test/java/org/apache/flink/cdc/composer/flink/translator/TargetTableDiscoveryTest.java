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

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.sink.SupportsTargetTableDiscovery;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.source.SupportsTableDiscovery;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.RouteDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TargetTableDiscoveryTest {
    private static class UndiscoverableSource implements DataSource {
        @Override
        public EventSourceProvider getEventSourceProvider() {
            throw new AssertionError("Unexpected source access");
        }

        @Override
        public MetadataAccessor getMetadataAccessor() {
            throw new AssertionError("Unfiltered metadata must not be used");
        }
    }

    private static class DiscoverableSource extends UndiscoverableSource
            implements SupportsTableDiscovery {
        private List<TableId> tables;
        private int calls;
        private RuntimeException failure;

        @Override
        public List<TableId> listCapturedTables() {
            calls++;
            if (failure != null) {
                throw failure;
            }
            return tables;
        }
    }

    @Test
    void preservesUnroutedTablesAndProducesStableDistinctOrder() {
        RecordingSink sink = new RecordingSink(true);
        TargetTableDiscovery.initialize(
                pipeline(RouteMode.ALL_MATCH), source("db.z", "db.a", "db.z"), sink);
        assertThat(sink.tables).containsExactly(TableId.parse("db.a"), TableId.parse("db.z"));
    }

    @ParameterizedTest
    @EnumSource(RouteMode.class)
    void honorsRouteModeAndDeduplicatesMergedTargets(RouteMode mode) {
        RecordingSink sink = new RecordingSink(true);
        TargetTableDiscovery.initialize(
                pipeline(
                        mode,
                        new RouteDef("db.orders_[0-9]+", "sales.orders", null, null),
                        new RouteDef("db.orders_[0-9]+", "audit.orders", null, null)),
                source("db.orders_1", "db.orders_2", "db.unmatched"),
                sink);
        if (mode == RouteMode.ALL_MATCH) {
            assertThat(sink.tables)
                    .containsExactly(
                            TableId.parse("audit.orders"),
                            TableId.parse("db.unmatched"),
                            TableId.parse("sales.orders"));
        } else {
            assertThat(sink.tables)
                    .containsExactly(TableId.parse("db.unmatched"), TableId.parse("sales.orders"));
        }
    }

    @Test
    void appliesReplacementSymbolsAndRegexCaptureGroups() {
        RecordingSink sink = new RecordingSink(true);
        TargetTableDiscovery.initialize(
                pipeline(
                        RouteMode.ALL_MATCH,
                        new RouteDef("db.orders", "sales.<table>", "<table>", null),
                        new RouteDef("db.customers_([0-9]+)", "sales.customers_$1", null, null)),
                source("db.orders", "db.customers_7"),
                sink);
        assertThat(sink.tables)
                .containsExactly(TableId.parse("sales.customers_7"), TableId.parse("sales.orders"));
    }

    @Test
    void doesNotDiscoverWhenSinkDoesNotNeedTargets() {
        DiscoverableSource source = source("db.orders");
        TargetTableDiscovery.initialize(
                pipeline(RouteMode.ALL_MATCH), source, new RecordingSink(false));
        assertThat(source.calls).isZero();
        TargetTableDiscovery.initialize(
                pipeline(RouteMode.ALL_MATCH),
                source,
                new DataSink() {
                    @Override
                    public EventSinkProvider getEventSinkProvider() {
                        throw new AssertionError();
                    }

                    @Override
                    public MetadataApplier getMetadataApplier() {
                        throw new AssertionError();
                    }
                });
        assertThat(source.calls).isZero();
    }

    @Test
    void rejectsUnsupportedSourcesInsteadOfEnumeratingUnfilteredMetadata() {
        DataSource source = new UndiscoverableSource();
        assertThatThrownBy(
                        () ->
                                TargetTableDiscovery.initialize(
                                        pipeline(RouteMode.ALL_MATCH),
                                        source,
                                        new RecordingSink(true)))
                .hasMessageContaining("does not support submission-time table discovery");
    }

    @Test
    void preservesDiscoveryFailureCause() {
        DiscoverableSource source = new DiscoverableSource();
        source.failure = new IllegalStateException("metadata unavailable");
        assertThatThrownBy(
                        () ->
                                TargetTableDiscovery.initialize(
                                        pipeline(RouteMode.ALL_MATCH),
                                        source,
                                        new RecordingSink(true)))
                .hasMessageContaining("Cannot discover captured tables")
                .hasRootCauseMessage("metadata unavailable");
    }

    private static DiscoverableSource source(String... tables) {
        DiscoverableSource source = new DiscoverableSource();
        source.tables = Arrays.stream(tables).map(TableId::parse).collect(Collectors.toList());
        return source;
    }

    private static PipelineDef pipeline(RouteMode mode, RouteDef... routes) {
        Configuration config = new Configuration();
        config.set(PipelineOptions.PIPELINE_ROUTE_MODE, mode);
        return new PipelineDef(
                new SourceDef("test-source", null, new Configuration()),
                new SinkDef("test-sink", null, new Configuration()),
                Arrays.asList(routes),
                Collections.emptyList(),
                Collections.emptyList(),
                config);
    }

    private static class RecordingSink implements DataSink, SupportsTargetTableDiscovery {
        private final boolean discoveryRequired;
        private List<TableId> tables;

        private RecordingSink(boolean discoveryRequired) {
            this.discoveryRequired = discoveryRequired;
        }

        @Override
        public void discoverTargetTables(Supplier<List<TableId>> targetTables) {
            if (discoveryRequired) {
                tables = targetTables.get();
            }
        }

        @Override
        public EventSinkProvider getEventSinkProvider() {
            throw new UnsupportedOperationException();
        }

        @Override
        public MetadataApplier getMetadataApplier() {
            throw new UnsupportedOperationException();
        }
    }
}
