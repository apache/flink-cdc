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

package org.apache.flink.cdc.connectors.base.mocked.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.IncrementalSource;
import org.apache.flink.cdc.connectors.base.source.assigner.SplitAssigner;
import org.apache.flink.cdc.connectors.base.source.assigner.state.PendingSplitsState;
import org.apache.flink.cdc.connectors.base.source.enumerator.IncrementalSourceEnumerator;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;

import java.util.List;

/** Mocked {@link IncrementalSource}. */
public class MockedIncrementalSource extends IncrementalSource<String, MockedConfig> {

    public MockedIncrementalSource(
            SourceConfig.Factory<MockedConfig> configFactory,
            DebeziumDeserializationSchema<String> deserializationSchema) {
        super(
                configFactory,
                deserializationSchema,
                new MockedOffsetFactory(),
                new MockedDialect(configFactory));
    }

    @Override
    public SplitEnumerator<SourceSplitBase, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<SourceSplitBase> enumContext) {
        MockedConfig sourceConfig = configFactory.create(0);
        final SplitAssigner splitAssigner;
        if (!sourceConfig.getStartupOptions().isStreamOnly()) {
            try {
                final List<TableId> remainingTables =
                        dataSourceDialect.discoverDataCollections(sourceConfig);
                boolean isTableIdCaseSensitive =
                        dataSourceDialect.isDataCollectionIdCaseSensitive(sourceConfig);
                splitAssigner =
                        new MockedHybridSplitAssigner(
                                sourceConfig,
                                enumContext.currentParallelism(),
                                remainingTables,
                                isTableIdCaseSensitive,
                                dataSourceDialect,
                                offsetFactory,
                                enumContext);
            } catch (Exception e) {
                throw new FlinkRuntimeException(
                        "Failed to discover captured tables for enumerator", e);
            }
        } else {
            splitAssigner =
                    new MockedStreamSplitAssigner(
                            sourceConfig, dataSourceDialect, offsetFactory, enumContext);
        }

        return new IncrementalSourceEnumerator(
                enumContext,
                sourceConfig,
                splitAssigner,
                getBoundedness(),
                dataSourceDialect.getNumberOfStreamSplits(sourceConfig));
    }

    @VisibleForTesting
    public MockedDialect getDialect() {
        return (MockedDialect) dataSourceDialect;
    }
}
