package org.apache.flink.cdc.benchmark.cases;

import org.apache.flink.cdc.benchmark.common.PipelineBenchmarkBase;
import org.apache.flink.cdc.benchmark.common.PipelineBenchmarkOptions;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Collections;

public class RoutedPipeline extends PipelineBenchmarkBase {

    @Benchmark
    public void runRoutedPipelineBenchmarkTest(Blackhole blackhole) throws Exception {
        PipelineBenchmarkOptions options =
                new PipelineBenchmarkOptions.Builder()
                        .setParallelism(1).build();
        Arrays.stream(
                        runInPipeline(
                                Collections.singletonList(
                                        new CreateTableEvent(
                                                TableId.tableId("foo", "bar", "baz"),
                                                Schema.newBuilder()
                                                        .physicalColumn(
                                                                "id", DataTypes.BIGINT().notNull())
                                                        .physicalColumn(
                                                                "everything", DataTypes.STRING())
                                                        .primaryKey("id")
                                                        .build())),
                                options))
                .forEach(blackhole::consume);
    }
}
