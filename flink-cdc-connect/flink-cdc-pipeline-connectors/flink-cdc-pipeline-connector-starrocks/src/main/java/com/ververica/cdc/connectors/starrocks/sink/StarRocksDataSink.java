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

package com.ververica.cdc.connectors.starrocks.sink;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.v2.StarRocksSink;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.sink.DataSink;
import com.ververica.cdc.common.sink.EventSinkProvider;
import com.ververica.cdc.common.sink.FlinkSinkProvider;
import com.ververica.cdc.common.sink.MetadataApplier;

import java.io.Serializable;
import java.time.ZoneId;

/** A {@link DataSink} for StarRocks connector that supports schema evolution. */
public class StarRocksDataSink implements DataSink, Serializable {

    private static final long serialVersionUID = 1L;

    /** Configurations for sink connector. */
    private final StarRocksSinkOptions sinkOptions;

    /** Configurations for creating a StarRocks table. */
    private final TableCreateConfig tableCreateConfig;

    /** Configurations for schema change. */
    private final SchemaChangeConfig schemaChangeConfig;

    /**
     * The local time zone used when converting from <code>TIMESTAMP WITH LOCAL TIME ZONE</code>.
     */
    private final ZoneId zoneId;

    public StarRocksDataSink(
            StarRocksSinkOptions sinkOptions,
            TableCreateConfig tableCreateConfig,
            SchemaChangeConfig schemaChangeConfig,
            ZoneId zoneId) {
        this.sinkOptions = sinkOptions;
        this.tableCreateConfig = tableCreateConfig;
        this.schemaChangeConfig = schemaChangeConfig;
        this.zoneId = zoneId;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        StarRocksSink<Event> starRocksSink =
                SinkFunctionFactory.createSink(
                        sinkOptions, new EventRecordSerializationSchema(zoneId));
        return FlinkSinkProvider.of(starRocksSink);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        StarRocksCatalog catalog =
                new StarRocksCatalog(
                        sinkOptions.getJdbcUrl(),
                        sinkOptions.getUsername(),
                        sinkOptions.getPassword());
        return new StarRocksMetadataApplier(catalog, tableCreateConfig, schemaChangeConfig);
    }
}
