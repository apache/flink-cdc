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

package org.apache.flink.cdc.connectors.fluss.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussHybridSnapshotLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussLogSplitState;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitState;
import org.apache.flink.cdc.source.SingleThreadFetcherManagerAdapter;
import org.apache.flink.cdc.source.SingleThreadMultiplexSourceReaderBaseAdapter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A generic {@link org.apache.flink.api.connector.source.SourceReader} for Fluss, built on top of
 * Flink's {@link SingleThreadMultiplexSourceReaderBase}. This delegates the low-level split
 * management, fetcher lifecycle, and availability tracking to the base class, and only provides
 * Fluss-specific initialization logic.
 *
 * <p>The output type {@code T} is determined by the provided {@link FlussDeserializer}, making this
 * reader reusable for different output types.
 *
 * @param <T> The type of output records produced by this reader.
 */
public class FlussSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBaseAdapter<
                FlussSourceRecord, T, FlussSplitBase, FlussSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceReader.class);

    private final FlussRecordEmitter<T> recordEmitter;
    private final WrapperFlussMetricRegistry metricRegistry;

    public FlussSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> elementsQueue,
            SourceReaderContext readerContext,
            org.apache.fluss.config.Configuration flussConfig,
            WrapperFlussMetricRegistry metricRegistry,
            FlussSourceReaderMetrics sourceReaderMetrics,
            FlussRecordEmitter<T> recordEmitter) {
        super(
                elementsQueue,
                new SingleThreadFetcherManagerAdapter<FlussSourceRecord, FlussSplitBase>(
                        elementsQueue,
                        () ->
                                new FlussSplitReader(
                                        flussConfig, metricRegistry, sourceReaderMetrics)),
                recordEmitter,
                new Configuration(),
                readerContext);
        this.recordEmitter = recordEmitter;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void close() throws Exception {
        try {
            super.close();
        } finally {
            metricRegistry.close();
        }
    }

    @Override
    protected FlussSplitState initializedState(FlussSplitBase split) {
        // Restore deserializer schema caches from the recovered split (like MySQL's applySplit)
        recordEmitter.applySplit(split);
        if (split.isHybridSnapshotLogSplit()) {
            return new FlussHybridSnapshotLogSplitState(split.asHybridSnapshotLogSplit());
        } else if (split.isLogSplit()) {
            return new FlussLogSplitState(split.asLogSplit());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported split type: " + split.getClass().getSimpleName());
        }
    }

    @Override
    protected FlussSplitBase toSplitType(String splitId, FlussSplitState splitState) {
        return splitState.toFlussSplit();
    }

    @Override
    protected void onSplitFinished(Map<String, FlussSplitState> finishedSplitIds) {
        // Fluss source is continuous and unbounded; splits should not normally finish.
        LOG.info("Splits finished: {}", finishedSplitIds.keySet());
    }
}
