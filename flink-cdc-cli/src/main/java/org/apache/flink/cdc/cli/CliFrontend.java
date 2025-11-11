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

package org.apache.flink.cdc.cli;

import org.apache.flink.cdc.cli.utils.ConfigurationUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils.getFlinkHome;
import static org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils.mergeFlinkConfigurationsWithPriority;

/** The frontend entrypoint for the command-line interface of Flink CDC. */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";

    public static void main(String[] args) throws Exception {
        Options cliOptions = CliFrontendOptions.initializeOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(cliOptions, args);

        // Help message
        if (args.length == 0 || commandLine.hasOption(CliFrontendOptions.HELP)) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.setLeftPadding(4);
            formatter.setWidth(80);
            formatter.printHelp(" ", cliOptions);
            return;
        }

        // Create executor and execute the pipeline
        PipelineExecution.ExecutionInfo result = createExecutor(commandLine).run();

        // Print execution result
        printExecutionInfo(result);
    }

    @VisibleForTesting
    static CliExecutor createExecutor(CommandLine commandLine) throws Exception {
        // The pipeline definition file would remain unparsed
        List<String> unparsedArgs = commandLine.getArgList();
        if (unparsedArgs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing pipeline definition file path in arguments. ");
        }

        Path pipelineDefPath = new Path(unparsedArgs.get(0));
        // Take the first unparsed argument as the pipeline definition file
        LOG.info("Real Path pipelineDefPath {}", pipelineDefPath);
        // Global pipeline configuration
        Configuration globalPipelineConfig = getGlobalConfig(commandLine);

        Path flinkHome = getFlinkHome(commandLine);
        org.apache.flink.configuration.Configuration flinkConfig =
                mergeFlinkConfigurationsWithPriority(pipelineDefPath, commandLine);

        // Additional JARs
        List<Path> additionalJars =
                Arrays.stream(
                                Optional.ofNullable(
                                                commandLine.getOptionValues(CliFrontendOptions.JAR))
                                        .orElse(new String[0]))
                        .map(Path::new)
                        .collect(Collectors.toList());

        // Build executor
        return new CliExecutor(
                commandLine,
                pipelineDefPath,
                flinkConfig,
                globalPipelineConfig,
                additionalJars,
                flinkHome);
    }

    private static Configuration getGlobalConfig(CommandLine commandLine) throws Exception {
        // Try to get global config path from command line
        String globalConfig = commandLine.getOptionValue(CliFrontendOptions.GLOBAL_CONFIG);
        if (globalConfig != null) {
            Path globalConfigPath = new Path(globalConfig);
            LOG.info("Using global config in command line: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to Flink CDC home
        String flinkCdcHome = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        if (flinkCdcHome != null) {
            Path globalConfigPath =
                    new Path(
                            flinkCdcHome, Joiner.on(File.separator).join("conf", "flink-cdc.yaml"));
            LOG.info("Using global config in FLINK_CDC_HOME: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to empty configuration
        LOG.warn(
                "Cannot find global configuration in command-line or FLINK_CDC_HOME. Will use empty global configuration.");
        return new Configuration();
    }

    private static void printExecutionInfo(PipelineExecution.ExecutionInfo info) {
        System.out.println("Pipeline has been submitted to cluster.");
        System.out.printf("Job ID: %s\n", info.getId());
        System.out.printf("Job Description: %s\n", info.getDescription());
    }
}
