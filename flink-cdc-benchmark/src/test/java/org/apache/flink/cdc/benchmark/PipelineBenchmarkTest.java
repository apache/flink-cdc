package org.apache.flink.cdc.benchmark;

import org.apache.flink.cdc.benchmark.cases.RoutedPipeline;
import org.apache.flink.cdc.benchmark.cases.SchemaEvolvingPipeline;
import org.apache.flink.cdc.benchmark.cases.TransformedPipeline;
import org.apache.flink.cdc.benchmark.cases.VanillaPipeline;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

class PipelineBenchmarkTest {

    @Test
    void runPipelineBenchmarkTests() throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(VanillaPipeline.class.getSimpleName())
                        .include(TransformedPipeline.class.getSimpleName())
                        .include(RoutedPipeline.class.getSimpleName())
                        .include(SchemaEvolvingPipeline.class.getSimpleName())
                        .mode(Mode.AverageTime)
                        .warmupIterations(1)
                        .measurementIterations(5)
                        .forks(1)
                        .build();
        new Runner(opt).run();
    }
}
