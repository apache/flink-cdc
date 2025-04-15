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

package org.apache.flink.cdc.cli.utils;

import org.apache.flink.cdc.cli.CliFrontendOptions;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.RestoreMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.cdc.cli.CliFrontendOptions.FLINK_CONFIG;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_ALLOW_NON_RESTORED_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_CLAIM_MODE;
import static org.apache.flink.cdc.cli.CliFrontendOptions.SAVEPOINT_PATH_OPTION;
import static org.apache.flink.cdc.cli.CliFrontendOptions.TARGET;
import static org.apache.flink.cdc.cli.CliFrontendOptions.USE_MINI_CLUSTER;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.LOCAL;
import static org.apache.flink.cdc.composer.flink.deployment.ComposeDeployment.REMOTE;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.RESTORE_MODE;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE;
import static org.apache.flink.runtime.jobgraph.SavepointConfigOptions.SAVEPOINT_PATH;

/** Utilities for handling Flink configuration and environment. */
public class FlinkEnvironmentUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvironmentUtils.class);
    private static final String FLINK_HOME_ENV_VAR = "FLINK_HOME";
    private static final String FLINK_CONF_DIR = "conf";
    private static final String LEGACY_FLINK_CONF_FILENAME = "flink-conf.yaml";
    private static final String FLINK_CONF_FILENAME = "config.yaml";

    /**
     * Load and merge flink configuration from flink_home、command line and flink pipeline.yaml.flink
     * config priority: pipeline.yaml > command line > flink_home.
     */
    public static Configuration mergeFlinkConfigurationsWithPriority(
            Path pipelineDefPath, CommandLine commandLine) throws Exception {
        Configuration flinkConfig = loadBaseFlinkConfig(commandLine);

        mergeCommandLineFlinkConfig(commandLine, flinkConfig);

        mergePipelineFlinkConfig(pipelineDefPath, flinkConfig);

        applySavepointSettings(flinkConfig);
        return flinkConfig;
    }

    private static Configuration loadBaseFlinkConfig(CommandLine commandLine) throws Exception {
        Path flinkHome = getFlinkHome(commandLine);
        Map<String, String> flinkConfigurationMap;
        Path flinkConfPath =
                new Path(
                        flinkHome,
                        Joiner.on(File.separator).join(FLINK_CONF_DIR, FLINK_CONF_FILENAME));
        if (flinkConfPath.getFileSystem().exists(flinkConfPath)) {
            flinkConfigurationMap = ConfigurationUtils.loadConfigFile(flinkConfPath).toMap();
        } else {
            flinkConfigurationMap =
                    ConfigurationUtils.loadConfigFile(
                                    new Path(
                                            flinkHome,
                                            Joiner.on(File.separator)
                                                    .join(
                                                            FLINK_CONF_DIR,
                                                            LEGACY_FLINK_CONF_FILENAME)),
                                    true)
                            .toMap();
        }
        return Configuration.fromMap(flinkConfigurationMap);
    }

    private static void mergeCommandLineFlinkConfig(
            CommandLine commandLine, Configuration flinkConfig) {
        Properties commandLineProperties = commandLine.getOptionProperties(FLINK_CONFIG.getOpt());
        // Use "remote" as the default target
        String target =
                commandLine.hasOption(USE_MINI_CLUSTER)
                        ? LOCAL.getName()
                        : commandLine.getOptionValue(TARGET, REMOTE.getName());
        flinkConfig.set(DeploymentOptions.TARGET, target);

        Optional.ofNullable(commandLine.getOptionValue(SAVEPOINT_PATH_OPTION))
                .ifPresent(value -> flinkConfig.set(SAVEPOINT_PATH, value));

        Optional.ofNullable(commandLine.getOptionValue(SAVEPOINT_CLAIM_MODE))
                .ifPresent(
                        value -> {
                            RestoreMode restoreMode =
                                    org.apache.flink.configuration.ConfigurationUtils.convertValue(
                                            value,
                                            org.apache.flink.cdc.cli.utils.ConfigurationUtils
                                                    .getClaimModeClass());
                            flinkConfig.set(RESTORE_MODE, restoreMode);
                        });

        if (commandLine.hasOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION)) {
            flinkConfig.set(SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        }

        LOG.info("Dynamic flink config items found from command-line: {}", commandLineProperties);
        commandLineProperties.forEach(
                (key, value) -> validateAndApplyCommandLineEntry(flinkConfig, key, value));
    }

    private static void validateAndApplyCommandLineEntry(
            Configuration flinkConfig, Object key, Object value) {
        String keyStr = key.toString();
        String valueStr = value.toString();
        if (StringUtils.isNullOrWhitespaceOnly(keyStr)
                || StringUtils.isNullOrWhitespaceOnly(valueStr)) {
            throw new IllegalArgumentException(
                    String.format(
                            "null or white space argument for key or value: %s=%s", key, value));
        }
        flinkConfig.setString(keyStr.trim(), valueStr.trim());
    }

    private static void mergePipelineFlinkConfig(
            org.apache.flink.core.fs.Path pipelineDefPath, Configuration flinkConfig)
            throws IOException {
        Configuration flinkConfigFromPipelineDef =
                Configuration.fromMap(
                        YamlPipelineDefinitionParser.getFlinkConfigFromPipelineDef(
                                pipelineDefPath));
        LOG.info(
                "Dynamic flink config items found from flink pipeline.yaml: {}",
                flinkConfigFromPipelineDef);
        flinkConfig.addAll(flinkConfigFromPipelineDef);
    }

    private static void applySavepointSettings(Configuration flinkConfig) {
        SavepointRestoreSettings savepointSettings = createSavepointRestoreSettings(flinkConfig);
        SavepointRestoreSettings.toConfiguration(savepointSettings, flinkConfig);
    }

    public static Path getFlinkHome(CommandLine commandLine) {
        // Check command line arguments first
        String flinkHomeFromArgs = commandLine.getOptionValue(CliFrontendOptions.FLINK_HOME);
        if (flinkHomeFromArgs != null) {
            LOG.debug("Flink home is loaded by command-line argument: {}", flinkHomeFromArgs);
            return new Path(flinkHomeFromArgs);
        }

        // Fallback to environment variable
        String flinkHomeFromEnvVar = System.getenv(FLINK_HOME_ENV_VAR);
        if (flinkHomeFromEnvVar != null) {
            LOG.debug("Flink home is loaded by environment variable: {}", flinkHomeFromEnvVar);
            return new Path(flinkHomeFromEnvVar);
        }

        throw new IllegalArgumentException(
                "Cannot find Flink home from either command line arguments \"--flink-home\" "
                        + "or the environment variable \"FLINK_HOME\". "
                        + "Please make sure Flink home is properly set. ");
    }

    public static SavepointRestoreSettings createSavepointRestoreSettings(
            Configuration flinkConfig) {
        final Optional<String> savepointOpt = flinkConfig.getOptional(SAVEPOINT_PATH);
        if (!savepointOpt.isPresent()) {
            return SavepointRestoreSettings.none();
        }

        final String savepointPath = savepointOpt.get();
        LOG.info("Load savepoint from path: {}", savepointPath);

        final boolean allowNonRestoredState =
                flinkConfig
                        .getOptional(SAVEPOINT_IGNORE_UNCLAIMED_STATE)
                        .orElse(SAVEPOINT_IGNORE_UNCLAIMED_STATE.defaultValue());

        final Object restoreMode =
                flinkConfig.getOptional(RESTORE_MODE).orElse(RESTORE_MODE.defaultValue());

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
}
