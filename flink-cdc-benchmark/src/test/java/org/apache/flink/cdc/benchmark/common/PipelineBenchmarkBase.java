package org.apache.flink.cdc.benchmark.common;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSinkOptions;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSourceOptions;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

/** Abstract common class for running pipeline */
public abstract class PipelineBenchmarkBase {

    public String[] runInPipeline(List<Event> events, PipelineBenchmarkOptions options)
            throws Exception {
        PrintStream realOut = System.out;
        ByteArrayOutputStream outCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outCaptor));

        try {
            FlinkPipelineComposer composer = FlinkPipelineComposer.ofMiniCluster();

            // Setup value source
            Configuration sourceConfig = new Configuration();
            sourceConfig.set(
                    ValuesDataSourceOptions.EVENT_SET_ID,
                    ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);
            ValuesDataSourceHelper.setSourceEvents(Collections.singletonList(events));

            SourceDef sourceDef =
                    new SourceDef(ValuesDataFactory.IDENTIFIER, "Value Source", sourceConfig);

            // Setup value sink
            Configuration sinkConfig = new Configuration();
            sinkConfig.set(ValuesDataSinkOptions.MATERIALIZED_IN_MEMORY, true);
            SinkDef sinkDef = new SinkDef(ValuesDataFactory.IDENTIFIER, "Value Sink", sinkConfig);

            // Setup pipeline
            Configuration pipelineConfig = new Configuration();
            pipelineConfig.set(PipelineOptions.PIPELINE_PARALLELISM, options.getParallelism());
            pipelineConfig.set(
                    PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR,
                    options.getSchemaChangeBehavior());
            PipelineDef pipelineDef =
                    new PipelineDef(
                            sourceDef,
                            sinkDef,
                            options.getRouteDefs(),
                            options.getTransformDefs(),
                            Collections.emptyList(),
                            pipelineConfig);

            // Execute the pipeline
            PipelineExecution execution = composer.compose(pipelineDef);
            execution.execute();
            return outCaptor.toString().split(System.lineSeparator());
        } finally {
            System.setOut(realOut);
        }
    }
}
