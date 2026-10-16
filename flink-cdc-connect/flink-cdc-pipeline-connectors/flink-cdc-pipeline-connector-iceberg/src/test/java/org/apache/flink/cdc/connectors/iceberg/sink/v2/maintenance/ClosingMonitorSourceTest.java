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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.maintenance;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.MonitorSource;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ClosingMonitorSourceTest {
    @TempDir Path directory;

    @Test
    void retainsNativeCheckpointBytesAndClosesCatalogAfterPolling() throws Exception {
        try (HadoopCatalog catalog =
                new HadoopCatalog(
                        new org.apache.hadoop.conf.Configuration(), directory.toString())) {
            catalog.createTable(
                    TableIdentifier.of("sales", "orders"),
                    new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", directory.toString());
        properties.put("catalog-impl", MaintenanceRegressionTest.TrackingCatalog.class.getName());
        DeferredTableLoader loader =
                new DeferredTableLoader(
                        TableLoader.fromCatalog(
                                CatalogLoader.custom(
                                        "test",
                                        properties,
                                        new org.apache.hadoop.conf.Configuration(),
                                        properties.get("catalog-impl")),
                                TableIdentifier.of("sales", "orders")),
                        "sales.orders");
        MaintenanceRegressionTest.TrackingCatalog.opened = 0;
        MaintenanceRegressionTest.TrackingCatalog.closed = 0;
        checkMonitor(new MonitorSource(loader, RateLimiterStrategy.perSecond(1000), 10), loader);
        assertThat(MaintenanceRegressionTest.TrackingCatalog.opened).isEqualTo(1);
        assertThat(MaintenanceRegressionTest.TrackingCatalog.closed).isEqualTo(1);
    }

    @SuppressWarnings("unchecked")
    private static <SplitT extends SourceSplit> void checkMonitor(
            Source<TableChange, SplitT, Collection<SplitT>> nativeSource, TableLoader loader)
            throws Exception {
        ClosingMonitorSource<SplitT, Collection<SplitT>> source =
                new ClosingMonitorSource<>(nativeSource, loader);
        SplitEnumeratorContext<SplitT> enumerationContext =
                (SplitEnumeratorContext<SplitT>)
                        Proxy.newProxyInstance(
                                SplitEnumeratorContext.class.getClassLoader(),
                                new Class<?>[] {SplitEnumeratorContext.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("metricGroup")) {
                                        return UnregisteredMetricsGroup
                                                .createSplitEnumeratorMetricGroup();
                                    }
                                    if (method.getName().equals("currentParallelism")) {
                                        return 1;
                                    }
                                    throw new AssertionError(
                                            "Unexpected enumeration context call "
                                                    + method.getName());
                                });
        Collection<SplitT> checkpoint;
        try (SplitEnumerator<SplitT, Collection<SplitT>> enumerator =
                source.createEnumerator(enumerationContext)) {
            checkpoint = enumerator.snapshotState(1);
        }
        SimpleVersionedSerializer<Collection<SplitT>> nativeSerializer =
                nativeSource.getEnumeratorCheckpointSerializer();
        SimpleVersionedSerializer<Collection<SplitT>> serializer =
                source.getEnumeratorCheckpointSerializer();
        assertThat(serializer.getVersion()).isEqualTo(nativeSerializer.getVersion());
        assertThat(serializer.serialize(checkpoint))
                .isEqualTo(nativeSerializer.serialize(checkpoint));
        Collection<SplitT> restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(checkpoint));
        SourceReaderContext readerContext =
                (SourceReaderContext)
                        Proxy.newProxyInstance(
                                SourceReaderContext.class.getClassLoader(),
                                new Class<?>[] {SourceReaderContext.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("metricGroup")) {
                                        return UnregisteredMetricsGroup
                                                .createSourceReaderMetricGroup();
                                    }
                                    if (method.getName().equals("sendSplitRequest")) {
                                        return null;
                                    }
                                    if (method.getName().equals("getIndexOfSubtask")) {
                                        return 0;
                                    }
                                    throw new AssertionError(
                                            "Unexpected reader context call " + method.getName());
                                });
        List<TableChange> output = new ArrayList<>();
        ReaderOutput<TableChange> readerOutput =
                (ReaderOutput<TableChange>)
                        Proxy.newProxyInstance(
                                ReaderOutput.class.getClassLoader(),
                                new Class<?>[] {ReaderOutput.class},
                                (proxy, method, args) -> {
                                    if (method.getName().equals("collect")) {
                                        output.add((TableChange) args[0]);
                                        return null;
                                    }
                                    throw new AssertionError(
                                            "Unexpected reader output call " + method.getName());
                                });
        try (SourceReader<TableChange, SplitT> reader = source.createReader(readerContext)) {
            reader.start();
            // Deserialization binds the iterator to the worker's loader, as on recovery.
            reader.addSplits(new ArrayList<>(restored));
            reader.isAvailable().get(5, TimeUnit.SECONDS);
            reader.pollNext(readerOutput);
            assertThat(output).hasSize(1);
            assertThat(reader.snapshotState(2)).hasSize(1);
        }
    }
}
