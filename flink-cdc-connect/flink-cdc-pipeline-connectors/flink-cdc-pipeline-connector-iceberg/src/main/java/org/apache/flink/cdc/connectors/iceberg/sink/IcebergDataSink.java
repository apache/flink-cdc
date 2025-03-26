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

package org.apache.flink.cdc.connectors.iceberg.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.sink.EventSinkProvider;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergSink;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/** A {@link DataSink} for Apache Iceberg. */
public class IcebergDataSink implements DataSink, Serializable {

    // options for creating Iceberg catalog.
    private final Map<String, String> catalogOptions;

    // options for creating Iceberg table.
    private final Map<String, String> tableOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private final ZoneId zoneId;

    public final String schemaOperatorUid;

    public final CompactionOptions compactionOptions;

    public IcebergDataSink(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            ZoneId zoneId,
            String schemaOperatorUid,
            CompactionOptions compactionOptions) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.zoneId = zoneId;
        this.schemaOperatorUid = schemaOperatorUid;
        this.compactionOptions = compactionOptions;
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        IcebergSink icebergEventSink =
                new IcebergSink(catalogOptions, tableOptions, zoneId, compactionOptions);
        return FlinkSinkProvider.of(icebergEventSink);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new IcebergMetadataApplier(catalogOptions, tableOptions, partitionMaps);
    }
}
