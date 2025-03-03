package org.apache.flink.cdc.benchmark.cases;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

public class VanillaPipeline {

    @Benchmark
    public void runVanillaPipelineBenchmarkTest(Blackhole blackhole) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            sb.append("-> ").append(i);
        }
        blackhole.consume(sb.toString());
    }
}
