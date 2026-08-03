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

package org.apache.flink.cdc.connectors.fluss.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussDeserializer;
import org.apache.flink.cdc.connectors.fluss.source.enumerator.FlussSourceEnumState;
import org.apache.flink.cdc.connectors.fluss.source.enumerator.FlussSourceEnumStateSerializer;
import org.apache.flink.cdc.connectors.fluss.source.enumerator.FlussSourceEnumerator;
import org.apache.flink.cdc.connectors.fluss.source.metrics.FlussSourceReaderMetrics;
import org.apache.flink.cdc.connectors.fluss.source.reader.FlussRecordEmitter;
import org.apache.flink.cdc.connectors.fluss.source.reader.FlussSourceReader;
import org.apache.flink.cdc.connectors.fluss.source.reader.FlussSourceRecord;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitBase;
import org.apache.flink.cdc.connectors.fluss.source.split.FlussSplitSerializer;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.fluss.client.initializer.OffsetsInitializer;

import java.util.Collections;
import java.util.HashSet;

/**
 * A generic Flink {@link Source} implementation for reading records from Fluss tables. This source
 * is continuous and unbounded, reading change log records from the Fluss log store.
 *
 * <p>The output type {@code T} is determined by the provided {@link FlussDeserializer}, making this
 * source reusable for different output types. The CDC-specific binding (e.g., producing {@code
 * Event} objects) is done at the {@link FlussDataSource} layer.
 *
 * @param <T> The type of output records produced by this source.
 */
public class FlussSource<T> implements Source<T, FlussSplitBase, FlussSourceEnumState> {

    private static final long serialVersionUID = 1L;

    private final TableDiscoverer discoverer;
    private final org.apache.fluss.config.Configuration flussConfig;
    private final Configuration sourceConfig;
    private final OffsetsInitializer offsetsInitializer;
    private final long scanDiscoveryIntervalMs;
    private final FlussDeserializer<T> deserializer;

    public FlussSource(
            TableDiscoverer discoverer,
            org.apache.fluss.config.Configuration flussConfig,
            Configuration sourceConfig,
            OffsetsInitializer offsetsInitializer,
            long scanDiscoveryIntervalMs,
            FlussDeserializer<T> deserializer) {
        this.discoverer = discoverer;
        this.flussConfig = flussConfig;
        this.sourceConfig = sourceConfig;
        this.offsetsInitializer = offsetsInitializer;
        this.scanDiscoveryIntervalMs = scanDiscoveryIntervalMs;
        this.deserializer = deserializer;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FlussSplitBase, FlussSourceEnumState> createEnumerator(
            SplitEnumeratorContext<FlussSplitBase> enumContext) {
        return new FlussSourceEnumerator(
                enumContext,
                discoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                scanDiscoveryIntervalMs,
                new HashSet<>());
    }

    @Override
    public SplitEnumerator<FlussSplitBase, FlussSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<FlussSplitBase> enumContext, FlussSourceEnumState checkpoint) {
        return new FlussSourceEnumerator(
                enumContext,
                discoverer,
                flussConfig,
                sourceConfig,
                offsetsInitializer,
                scanDiscoveryIntervalMs,
                checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<FlussSplitBase> getSplitSerializer() {
        return new FlussSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FlussSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new FlussSourceEnumStateSerializer();
    }

    @Override
    public SourceReader<T, FlussSplitBase> createReader(SourceReaderContext readerContext) {
        FlussSourceReaderMetrics sourceReaderMetrics =
                new FlussSourceReaderMetrics(readerContext.metricGroup());
        WrapperFlussMetricRegistry metricRegistry =
                new WrapperFlussMetricRegistry(readerContext.metricGroup(), Collections.emptySet());

        FlussRecordEmitter<T> recordEmitter = new FlussRecordEmitter<>(deserializer);
        FutureCompletingBlockingQueue<RecordsWithSplitIds<FlussSourceRecord>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        return new FlussSourceReader<>(
                elementsQueue,
                readerContext,
                flussConfig,
                metricRegistry,
                sourceReaderMetrics,
                recordEmitter);
    }
}
