package org.apache.flink.cdc.cli;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineComposer;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.composer.parser.YamlPipelineDefinitionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PipelineEmbeddedExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PipelineEmbeddedExecutor.class);

    private final Path pipelineDefPath;

    public PipelineEmbeddedExecutor(Path pipelineDefPath) {
        this.pipelineDefPath = pipelineDefPath;
    }

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        PipelineEmbeddedExecutor executor = new PipelineEmbeddedExecutor(Paths.get(params.get("pipeline-file")));
        PipelineExecution.ExecutionInfo executionInfo = executor.run();
        logExecutionInfo(executionInfo);
    }

    public PipelineExecution.ExecutionInfo run() throws Exception {
        // Parse pipeline definition file
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(pipelineDefPath, new Configuration());

        // Create composer
        PipelineComposer composer = FlinkPipelineComposer.ofApplicationCluster();

        // Compose pipeline
        PipelineExecution execution = composer.compose(pipelineDef);

        // Execute the pipeline
        return execution.execute();
    }

    private static void logExecutionInfo(PipelineExecution.ExecutionInfo info) {
        LOG.info("Pipeline has been submitted to cluster.");
        LOG.info("Job ID: {}\n", info.getId());
        LOG.info("Job Description: {}\n", info.getDescription());
    }
}
