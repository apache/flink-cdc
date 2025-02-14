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

import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.cli.utils.ConfigurationUtils;
import org.apache.flink.cdc.cli.utils.FlinkEnvironmentUtils;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.composer.PipelineExecution;
import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.client.deployment.executors.RemoteExecutor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.cli.CliFrontendOptions.FLINK_CONFIG;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_CLAIM_MODE;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_PATH_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.TARGET;
import static org.apache.flink.cdc.cli.CliFrontendOptions.USE_MINI_CLUSTER;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.RESTORE_MODE;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH;

/** The frontend entrypoint for the command-line interface of Flink CDC. */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private static final String FLINK_HOME_ENV_VAR = "FLINK_HOME";
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

        Path pipelineDefPath = Paths.get(unparsedArgs.get(0));
        // Take the first unparsed argument as the pipeline definition file
        LOG.info("Real Path pipelineDefPath {}", pipelineDefPath);
        // Global pipeline configuration
        Configuration globalPipelineConfig = getGlobalConfig(commandLine);

        org.apache.flink.configuration.Configuration flinkConfig =
                loadAndMergeFlinkConfig(pipelineDefPath, commandLine);

        // Additional JARs
        List<Path> additionalJars =
                Arrays.stream(
                                Optional.ofNullable(
                                                commandLine.getOptionValues(CliFrontendOptions.JAR))
                                        .orElse(new String[0]))
                        .map(Paths::get)
                        .collect(Collectors.toList());

        // Build executor
        return new CliExecutor(
                commandLine, pipelineDefPath, flinkConfig, globalPipelineConfig, additionalJars);
    }

    /**
     * Load and merge flink configuration from flink_home、command line and flink pipeline.yaml.flink
     * config priority: pipeline.yaml > command line > flink_home.
     */
    private static org.apache.flink.configuration.Configuration loadAndMergeFlinkConfig(
            Path pipelineDefPath, CommandLine commandLine) throws Exception {
        // load Flink configuration from flink_home
        Path flinkHome = getFlinkHome(commandLine);
        org.apache.flink.configuration.Configuration flinkConfig =
                org.apache.flink.configuration.Configuration.fromMap(
                        FlinkEnvironmentUtils.loadFlinkConfiguration(flinkHome).toMap());

        // load flink configuration from command line,such as -Dxxx=xx, --target and
        // --use-mini-cluster
        Properties commandLineProperties = commandLine.getOptionProperties(FLINK_CONFIG.getOpt());
        // Use "remote" as the default target
        flinkConfig.set(
                DeploymentOptions.TARGET, commandLine.getOptionValue(TARGET, RemoteExecutor.NAME));
        if (commandLine.hasOption(USE_MINI_CLUSTER)) {
            flinkConfig.set(DeploymentOptions.TARGET, LocalExecutor.NAME);
        }
        LOG.info("Dynamic flink config items found from command-line: {}", commandLineProperties);
        commandLineProperties
                .stringPropertyNames()
                .forEach(
                        key ->
                                processFlinkConfigEntry(
                                        flinkConfig, key, commandLineProperties.getProperty(key)));

        // load flink configuration from pipeline.yaml
        org.apache.flink.configuration.Configuration flinkConfigFromPipelineDef =
                org.apache.flink.configuration.Configuration.fromMap(
                        YamlPipelineDefinitionParser.getFlinkConfigFromPipelineDef(
                                pipelineDefPath));
        LOG.info(
                "Dynamic flink config items found from flink pipeline.yaml: {}",
                flinkConfigFromPipelineDef);
        flinkConfig.addAll(flinkConfigFromPipelineDef);

        // load savepoint settings
        SavepointRestoreSettings savepointSettings =
                createSavepointRestoreSettings(commandLine, flinkConfig);
        SavepointRestoreSettings.toConfiguration(savepointSettings, flinkConfig);
        return flinkConfig;
    }

    private static void processFlinkConfigEntry(
            org.apache.flink.configuration.Configuration flinkConfig, String key, String value) {
        if (StringUtils.isNullOrWhitespaceOnly(key) || StringUtils.isNullOrWhitespaceOnly(value)) {
            throw new IllegalArgumentException(
                    String.format(
                            "null or white space argument for key or value: %s=%s", key, value));
        }
        String trimmedKey = key.trim();
        String trimmedValue = value.trim();
        ConfigOption<String> configOption =
                ConfigOptions.key(trimmedKey).stringType().defaultValue(trimmedValue);
        flinkConfig.set(configOption, trimmedValue);
        LOG.info("Dynamic flink config items found {}={}", trimmedKey, trimmedValue);
    }

    private static SavepointRestoreSettings createSavepointRestoreSettings(
            CommandLine commandLine, org.apache.flink.configuration.Configuration flinkConfig) {

        final String savepointPath =
                flinkConfig.contains(SAVEPOINT_PATH)
                        ? flinkConfig.get(SAVEPOINT_PATH)
                        : commandLine.getOptionValue(SAVEPOINT_PATH_OPTION.getOpt());

        if (savepointPath == null) {
            return SavepointRestoreSettings.none();
        }

        LOG.info("Load savepoint from path: {}", savepointPath);

        final boolean allowNonRestoredState =
                flinkConfig.contains(SAVEPOINT_IGNORE_UNCLAIMED_STATE)
                        ? flinkConfig.get(SAVEPOINT_IGNORE_UNCLAIMED_STATE)
                        : commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION.getOpt());

        final Object restoreMode;
        final String claimModeKey =
                flinkConfig.contains(RESTORE_MODE)
                        ? flinkConfig.getValue(RESTORE_MODE)
                        : commandLine.getOptionValue(SAVEPOINT_CLAIM_MODE.getOpt());

        if (claimModeKey != null) {
            restoreMode =
                    org.apache.flink.configuration.ConfigurationUtils.convertValue(
                            claimModeKey, ConfigurationUtils.getClaimModeClass());
        } else {
            restoreMode = SavepointConfigOptions.RESTORE_MODE.defaultValue();
        }

        return (SavepointRestoreSettings)
                Arrays.stream(SavepointRestoreSettings.class.getMethods())
                        .filter(
                                method ->
                                        method.getName().equals("forPath")
                                                && method.getParameterCount() == 3)
                        .findFirst()
                        .map(
                                method -> {
                                    try {
                                        return method.invoke(
                                                null,
                                                savepointPath,
                                                allowNonRestoredState,
                                                restoreMode);
                                    } catch (IllegalAccessException | InvocationTargetException e) {
                                        throw new RuntimeException(
                                                "Failed to invoke SavepointRestoreSettings#forPath nethod.",
                                                e);
                                    }
                                })
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                "Failed to resolve SavepointRestoreSettings#forPath method."));
    }

    private static Path getFlinkHome(CommandLine commandLine) {
        // Check command line arguments first
        String flinkHomeFromArgs = commandLine.getOptionValue(CliFrontendOptions.FLINK_HOME);
        if (flinkHomeFromArgs != null) {
            LOG.debug("Flink home is loaded by command-line argument: {}", flinkHomeFromArgs);
            return Paths.get(flinkHomeFromArgs);
        }

        // Fallback to environment variable
        String flinkHomeFromEnvVar = System.getenv(FLINK_HOME_ENV_VAR);
        if (flinkHomeFromEnvVar != null) {
            LOG.debug("Flink home is loaded by environment variable: {}", flinkHomeFromEnvVar);
            return Paths.get(flinkHomeFromEnvVar);
        }

        throw new IllegalArgumentException(
                "Cannot find Flink home from either command line arguments \"--flink-home\" "
                        + "or the environment variable \"FLINK_HOME\". "
                        + "Please make sure Flink home is properly set. ");
    }

    private static Configuration getGlobalConfig(CommandLine commandLine) throws Exception {
        // Try to get global config path from command line
        String globalConfig = commandLine.getOptionValue(CliFrontendOptions.GLOBAL_CONFIG);
        if (globalConfig != null) {
            Path globalConfigPath = Paths.get(globalConfig);
            LOG.info("Using global config in command line: {}", globalConfigPath);
            return ConfigurationUtils.loadConfigFile(globalConfigPath);
        }

        // Fallback to Flink CDC home
        String flinkCdcHome = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        if (flinkCdcHome != null) {
            Path globalConfigPath =
                    Paths.get(flinkCdcHome).resolve("conf").resolve("flink-cdc.yaml");
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
