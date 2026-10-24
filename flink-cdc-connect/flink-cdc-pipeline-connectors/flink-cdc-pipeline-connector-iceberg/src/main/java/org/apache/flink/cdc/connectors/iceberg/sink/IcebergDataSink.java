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
import org.apache.flink.cdc.common.sink.SupportsStreamGraphPostProcessing;
import org.apache.flink.cdc.common.sink.SupportsTargetTableDiscovery;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.IcebergSink;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance.MaintenanceGraphAdapter;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance.MaintenanceOptions;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** A {@link DataSink} for Apache Iceberg. */
public class IcebergDataSink
        implements DataSink,
                SupportsTargetTableDiscovery,
                SupportsStreamGraphPostProcessing,
                Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergDataSink.class);

    // options for creating Iceberg catalog.
    private final Map<String, String> catalogOptions;

    // options for creating Iceberg table.
    private final Map<String, String> tableOptions;

    // options for Hadoop configuration.
    private final Map<String, String> hadoopConfOptions;

    private final Map<TableId, List<String>> partitionMaps;

    private final ZoneId zoneId;

    private MaintenanceOptions maintenanceOptions;
    private final MaintenanceGraphAdapter maintenanceGraphAdapter = new MaintenanceGraphAdapter();

    public final String schemaOperatorUid;

    public final CompactionOptions compactionOptions;

    public final String jobIdPrefix;

    public IcebergDataSink(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            ZoneId zoneId,
            String schemaOperatorUid,
            CompactionOptions compactionOptions,
            String jobIdPrefix,
            Map<String, String> hadoopConfOptions) {
        this(
                catalogOptions,
                tableOptions,
                partitionMaps,
                zoneId,
                schemaOperatorUid,
                compactionOptions,
                jobIdPrefix,
                hadoopConfOptions,
                MaintenanceOptions.disabled());
    }

    public IcebergDataSink(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            Map<TableId, List<String>> partitionMaps,
            ZoneId zoneId,
            String schemaOperatorUid,
            CompactionOptions compactionOptions,
            String jobIdPrefix,
            Map<String, String> hadoopConfOptions,
            MaintenanceOptions maintenanceOptions) {
        this.maintenanceOptions = maintenanceOptions;
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.partitionMaps = partitionMaps;
        this.zoneId = zoneId;
        this.schemaOperatorUid = schemaOperatorUid;
        this.compactionOptions = compactionOptions;
        this.jobIdPrefix = jobIdPrefix;
        this.hadoopConfOptions = hadoopConfOptions;
    }

    @Override
    public void discoverTargetTables(Supplier<List<TableId>> targetTables) {
        if (maintenanceOptions.requiresTableDiscovery()) {
            try {
                maintenanceOptions = maintenanceOptions.withDiscoveredTables(targetTables.get());
            } catch (RuntimeException e) {
                throw new IllegalArgumentException(
                        "Cannot derive Iceberg maintenance target tables before submission. "
                                + "Check source discovery and routing, or configure sink.maintenance.tables explicitly.",
                        e);
            }
            LOG.info(
                    "Discovered Iceberg maintenance target tables: {}",
                    maintenanceOptions.tables());
        }
    }

    @Override
    public EventSinkProvider getEventSinkProvider() {
        IcebergSink icebergEventSink =
                new IcebergSink(
                        catalogOptions,
                        tableOptions,
                        zoneId,
                        compactionOptions,
                        jobIdPrefix,
                        hadoopConfOptions,
                        maintenanceOptions,
                        maintenanceGraphAdapter);
        return FlinkSinkProvider.of(icebergEventSink);
    }

    @Override
    public boolean requiresStreamGraphPostProcessing() {
        return maintenanceOptions.get(MaintenanceOptions.ENABLED);
    }

    @Override
    public void postProcessStreamGraph(StreamGraph graph) {
        maintenanceGraphAdapter.postProcessStreamGraph(graph);
    }

    @Override
    public MetadataApplier getMetadataApplier() {
        return new IcebergMetadataApplier(
                catalogOptions, tableOptions, partitionMaps, hadoopConfOptions);
    }

    public MaintenanceOptions getMaintenanceOptions() {
        return maintenanceOptions;
    }

    public Map<String, String> getHadoopConfOptions() {
        return hadoopConfOptions;
    }
}
