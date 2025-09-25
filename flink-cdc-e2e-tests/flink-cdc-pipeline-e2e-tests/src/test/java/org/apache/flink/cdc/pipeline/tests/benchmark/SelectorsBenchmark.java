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

package org.apache.flink.cdc.pipeline.tests.benchmark;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Selectors;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for table selector performance with and without cache.
 *
 * <pre>
 * Benchmark                                                   Mode  Cnt     Score     Error   Units
 * SelectorsBenchmark.testSelectorWithCache                   thrpt   20  1028.979 ± 218.663  ops/ms
 * SelectorsBenchmark.testSelectorWithoutCache                thrpt   20   136.747 ±  11.872  ops/ms
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@Threads(2)
public class SelectorsBenchmark {

    private Selectors selectors;
    private List<TableId> queryTableIds;

    @Setup(Level.Trial)
    public void setup() {
        selectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(
                                "test_wms_inventory_[a-z]+.inventory_batch_detail,"
                                        + "test_wms_inventory_[a-z]+.inventory_batch_detail_record,"
                                        + "test_wms_inventory_[a-z]+.inventory_batch_input,"
                                        + "test_wms_inventory_[a-z]+.inventory_flow_volume_level,"
                                        + "test_wms_inventory_[a-z]+.inventory_snapshot,"
                                        + "test_wms_log_[a-z]+.log_common_log_[a-z]+")
                        .build();

        queryTableIds =
                ImmutableList.of(
                        TableId.tableId(
                                "test_wms_common_europe.occupy_strategy_exe_progress_order"),
                        TableId.tableId("test_wms_common_europe.wave_strategy_rule_relation"),
                        TableId.tableId("db.sc2.A1"),
                        TableId.tableId("db2.sc2.A1"),
                        TableId.tableId("test_wms_output_s.out_moment_storage_location_relation"),
                        TableId.tableId("test_wms_output_a.out_moment_storage_location_relation"));

        // warm up cache
        for (TableId id : queryTableIds) {
            selectors.isMatch(id);
        }
    }

    /**
     * Benchmark to evaluate the performance of table selector with caching enabled.
     *
     * <p>This benchmark measures throughput when using a pre-built {@link Selectors} instance that
     * leverages internal caching mechanisms. This simulates a typical usage scenario where selector
     * rules are initialized once and reused across multiple queries.
     *
     * <p>Expected to perform significantly better than non-cached version due to avoidance of
     * repeated regex parsing and compilation.
     */
    @Benchmark
    public void testSelectorWithCache() {
        for (TableId id : queryTableIds) {
            selectors.isMatch(id);
        }
    }

    /**
     * Benchmark to evaluate the performance of table selector without using cache.
     *
     * <p>This benchmark constructs a new {@link Selectors} instance for each invocation, simulating
     * a cold-start or ad-hoc usage scenario. The overhead includes pattern parsing and matcher
     * construction, which significantly impacts throughput.
     *
     * <p>Useful for understanding worst-case performance and comparing against the cached version.
     */
    @Benchmark
    public void testSelectorWithoutCache() {
        Selectors freshSelectors =
                new Selectors.SelectorsBuilder()
                        .includeTables(
                                "test_wms_inventory_[a-z]+.inventory_batch_detail,"
                                        + "test_wms_inventory_[a-z]+.inventory_batch_detail_record,"
                                        + "test_wms_inventory_[a-z]+.inventory_batch_input,"
                                        + "test_wms_inventory_[a-z]+.inventory_flow_volume_level,"
                                        + "test_wms_inventory_[a-z]+.inventory_snapshot,"
                                        + "test_wms_log_[a-z]+.log_common_log_[a-z]+")
                        .build();
        for (TableId id : queryTableIds) {
            freshSelectors.isMatch(id);
        }
    }

    public static void main(String[] args) throws Exception {
        Options options =
                new OptionsBuilder()
                        .include(SelectorsBenchmark.class.getSimpleName())
                        .detectJvmArgs()
                        .build();
        new Runner(options).run();
    }
}
