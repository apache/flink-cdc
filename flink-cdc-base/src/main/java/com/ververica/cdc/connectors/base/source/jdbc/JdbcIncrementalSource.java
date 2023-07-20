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

package com.ververica.cdc.connectors.base.source.jdbc;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.config.JdbcSourceConfigFactory;
import com.ververica.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import com.ververica.cdc.connectors.base.options.StartupMode;
import com.ververica.cdc.connectors.base.source.IncrementalSource;
import com.ververica.cdc.connectors.base.source.assigner.HybridSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.SplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.StreamSplitAssigner;
import com.ververica.cdc.connectors.base.source.assigner.state.HybridPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.SnapshotPendingSplitsState;
import com.ververica.cdc.connectors.base.source.assigner.state.StreamPendingSplitsState;
import com.ververica.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import com.ververica.cdc.connectors.base.source.enumerator.SnapshotSourceEnumerator;
import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.relational.TableId;

import java.util.List;

/**
 * The basic source of Incremental Snapshot framework for JDBC datasource, it is based on FLIP-27
 * and Watermark Signal Algorithm which supports parallel reading snapshot of table and then
 * continue to capture data change by streaming reading.
 */
public class JdbcIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig> {

    public JdbcIncrementalSource(
            JdbcSourceConfigFactory configFactory,
            DebeziumDeserializationSchema<T> deserializationSchema,
            OffsetFactory offsetFactory,
            JdbcDataSourceDialect dataSourceDialect) {
        super(configFactory, deserializationSchema, offsetFactory, dataSourceDialect);
    }

    @Override
    public Boundedness getBoundedness() {
        JdbcSourceConfig sourceConfig = configFactory.create(0);
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.SNAPSHOT_ONLY) {
            return Boundedness.BOUNDED;
        } else {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext) {
        JdbcSourceConfig sourceConfig = configFactory.create(0);
        final SplitAssigner splitAssigner;
        if (sourceConfig.getStartupOptions().startupMode == StartupMode.SNAPSHOT_ONLY) {
            final List<TableId> remainingTables =
                    dataSourceDialect.discoverDataCollections(sourceConfig);
            boolean isTableIdCaseSensitive =
                    dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
            splitAssigner =
                    new SnapshotSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            remainingTables,
                            isTableIdCaseSensitive,
                            dataSourceDialect,
                            offsetFactory);
            return new SnapshotSourceEnumerator(enumContext, splitAssigner);
        }

        if (sourceConfig.getStartupOptions().startupMode == StartupMode.INITIAL) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new HybridSplitAssigner<>(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner = new StreamSplitAssigner(sourceConfig, dataSourceDialect, offsetFactory);
        }

        return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext, PendingSplitsState checkpoint) {
        JdbcSourceConfig sourceConfig = configFactory.create(0);

        final SplitAssigner splitAssigner;
        if (checkpoint instanceof SnapshotPendingSplitsState) {
            splitAssigner =
                    new SnapshotSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (SnapshotPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
            return new SnapshotSourceEnumerator(enumContext, splitAssigner);
        }
        if (checkpoint instanceof HybridPendingSplitsState) {
            splitAssigner =
                    new HybridSplitAssigner<>(
                            sourceConfig,
                            enumContext.currentParallelism(),
                            (HybridPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else if (checkpoint instanceof StreamPendingSplitsState) {
            splitAssigner =
                    new StreamSplitAssigner(
                            sourceConfig,
                            (StreamPendingSplitsState) checkpoint,
                            dataSourceDialect,
                            offsetFactory);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored PendingSplitsState: " + checkpoint);
        }
        return new IncrementalSourceEnumerator(enumContext, sourceConfig, splitAssigner);
    }
}
