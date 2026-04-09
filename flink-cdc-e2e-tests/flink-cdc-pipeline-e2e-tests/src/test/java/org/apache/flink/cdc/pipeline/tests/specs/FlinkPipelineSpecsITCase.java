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

package org.apache.flink.cdc.pipeline.tests.specs;

import org.apache.flink.cdc.cli.utils.YamlParserUtils;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** YAML-spec based pipeline test cases. */
public class FlinkPipelineSpecsITCase extends PipelineTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkPipelineSpecsITCase.class);
    private static final String MYSQL_CONTAINER_ALIAS = "mysql";

    static Stream<Arguments> loadTestSpecs() throws IOException {
        Path path = Paths.get("").toAbsolutePath().resolve("src/test/resources/rules");

        try (Stream<Path> dependencyResources = Files.walk(path)) {
            List<Path> p = dependencyResources.sorted().collect(Collectors.toList());
            return p.stream()
                    .filter(f -> f.getFileName().toString().endsWith(".yaml"))
                    .map(FlinkPipelineSpecsITCase::parseSpec)
                    .map(spec -> Arguments.of(spec.groupName, spec.specName, spec));
        }
    }

    private final String databaseName = "spec_db_" + UUID.randomUUID().toString().substring(0, 8);
    private final Function<String, String> dbNameFormatter =
            s -> s.replace("$database$", databaseName);

    private Connection getConnection(String databaseName) throws Exception {
        return DriverManager.getConnection(
                String.format(
                        "jdbc:mysql://%s:%s/%s?allowMultiQueries=true",
                        MYSQL.getHost(), MYSQL.getDatabasePort(), databaseName),
                MYSQL_TEST_USER,
                MYSQL_TEST_PASSWORD);
    }

    @BeforeEach
    void initializeDatabase() throws Exception {
        try (Connection conn = getConnection("");
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + databaseName);
        }
    }

    @AfterEach
    void destroyDatabase() throws Exception {
        try (Connection conn = getConnection("");
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
    }

    @ParameterizedTest(name = "{0} :: {1}")
    @MethodSource("loadTestSpecs")
    void runTestSpec(String groupName, String specName, RuleSpec spec) throws Exception {
        LOG.info("Running test: {} :: {}", groupName, specName);
        for (SpecStep step : spec.steps) {
            if (step instanceof ExecStep) {
                ExecStep execStep = (ExecStep) step;
                LOG.info("ExecStep: Executing SQL:\n{}", execStep.sql);
                try (Connection conn = getConnection(databaseName);
                        Statement stmt = conn.createStatement()) {
                    stmt.execute(execStep.sql);
                }
            } else if (step instanceof SubmitStep) {
                SubmitStep submitStep =
                        ((SubmitStep) step)
                                .substitute("$hostname$", MYSQL_CONTAINER_ALIAS)
                                .substitute("$database$", databaseName)
                                .substitute("$username$", MYSQL_TEST_USER)
                                .substitute("$password$", MYSQL_TEST_PASSWORD);

                if (submitStep.expectError.isEmpty()) {
                    LOG.info("SubmitStep: Submitting YAML Job:\n{}", submitStep.yaml);
                    submitPipelineJob(submitStep.yaml);
                } else {
                    LOG.info(
                            "SubmitStep: Submitting YAML Job:\n{}\nwith expected error: {}",
                            submitStep.yaml,
                            submitStep.expectError);

                    Throwable caughtError = null;
                    try {
                        submitPipelineJob(submitStep.yaml);
                    } catch (Throwable t) {
                        caughtError = t;
                    }

                    Assertions.assertThat(caughtError).isNotNull();
                    for (String error : submitStep.expectError) {
                        Assertions.assertThat(caughtError).hasStackTraceContaining(error);
                    }
                }
            } else if (step instanceof CheckStep) {
                CheckStep checkStep = (CheckStep) step;
                LOG.info("CheckStep: Going to perform checking:");
                LOG.info(" - Expected JM: {}", checkStep.jmLogs);
                LOG.info(" - Expected TM: {}", checkStep.tmLogs);
                if (!checkStep.jmLogs.isEmpty()) {
                    validateResult(
                            jobManagerConsumer,
                            dbNameFormatter,
                            checkStep.jmLogs.toArray(new String[0]));
                }
                if (!checkStep.tmLogs.isEmpty()) {
                    validateResult(dbNameFormatter, checkStep.tmLogs.toArray(new String[0]));
                }
            } else {
                Assertions.fail("Unexpected RuleSpec step: " + step);
            }
        }
    }

    private static RuleSpec parseSpec(Path yamlFile) {
        Map<String, Object> spec;
        try {
            spec = YamlParserUtils.loadYamlFile(yamlFile.toFile());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertThat(spec.get("steps")).isInstanceOf(List.class);
        String groupName = (String) spec.get("group");
        String specName = (String) spec.get("spec");
        List<SpecStep> parsedSteps = new ArrayList<>();

        List<Map<String, ?>> steps = (List<Map<String, ?>>) spec.get("steps");
        for (Map<String, ?> step : steps) {
            switch ((String) step.get("type")) {
                case "exec":
                    parsedSteps.add(new ExecStep((String) step.get("sql")));
                    break;
                case "submit":
                    parsedSteps.add(
                            new SubmitStep((String) step.get("yaml"), getLines(step.get("error"))));
                    break;
                case "check":
                    List<String> jmLogs = getLines(step.get("jm"));
                    List<String> tmLogs = getLines(step.get("tm"));
                    parsedSteps.add(new CheckStep(jmLogs, tmLogs));
                    break;
            }
        }
        return new RuleSpec(groupName, specName, parsedSteps);
    }

    private static List<String> getLines(@Nullable Object input) {
        return Optional.ofNullable(input)
                .map(String.class::cast)
                .map(s -> s.split("\n"))
                .map(Arrays::asList)
                .orElseGet(Collections::emptyList);
    }
}
