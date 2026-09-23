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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.source.discover.TableDiscoverer;
import org.apache.flink.cdc.connectors.fluss.source.deserializer.FlussRecordDeserializer;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;

/**
 * A CDC-specific {@link DataSource} implementation for Fluss. This is the layer where the generic
 * {@link FlussSource} is bound to CDC types by providing a {@link FlussRecordDeserializer} that
 * produces {@link Event} objects.
 */
public class FlussDataSource implements DataSource {

    private final Configuration flussConfig;
    private final org.apache.flink.cdc.common.configuration.Configuration sourceConfig;
    private final TableDiscoverer discoverer;
    private final OffsetsInitializer offsetsInitializer;
    private final long scanDiscoveryIntervalMs;

    public FlussDataSource(
            Configuration flussConfig,
            org.apache.flink.cdc.common.configuration.Configuration sourceConfig,
            TableDiscoverer discoverer,
            OffsetsInitializer offsetsInitializer,
            long scanDiscoveryIntervalMs) {
        this.flussConfig = flussConfig;
        this.sourceConfig = sourceConfig;
        this.discoverer = discoverer;
        this.offsetsInitializer = offsetsInitializer;
        this.scanDiscoveryIntervalMs = scanDiscoveryIntervalMs;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        return FlinkSourceProvider.of(
                new FlussSource<>(
                        discoverer,
                        flussConfig,
                        sourceConfig,
                        offsetsInitializer,
                        scanDiscoveryIntervalMs,
                        new FlussRecordDeserializer()));
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new FlussMetadataAccessor(flussConfig);
    }

    @Override
    public boolean isParallelMetadataSource() {
        return true;
    }
}
