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

package org.apache.flink.cdc.cli.guard;

import org.apache.flink.cdc.cli.parser.PipelineDefinitionParser;
import org.apache.flink.cdc.cli.parser.YamlPipelineDefinitionParser;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.exceptions.GuardVerificationException;
import org.apache.flink.cdc.composer.definition.PipelineDef;

import org.apache.flink.shaded.guava31.com.google.common.io.Resources;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.file.Paths;

/** Testcases for {@link PipelineDefGuard}. */
public class PipelineDefGuardTest {

    private void verifyPipelineYaml(String... name) throws Exception {
        URL resource =
                Resources.getResource(String.format("guard/%s.yaml", String.join("/", name)));
        PipelineDefinitionParser pipelineDefinitionParser = new YamlPipelineDefinitionParser();
        PipelineDef pipelineDef =
                pipelineDefinitionParser.parse(Paths.get(resource.getPath()), new Configuration());
        PipelineDefGuard.verify(pipelineDef);
    }

    @Test
    void testLegalCases() {
        Assertions.assertThatCode(() -> verifyPipelineYaml("legal", "legal-full"))
                .doesNotThrowAnyException();
    }

    @Test
    void testRouteGuard() {
        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "source-missing"))
                .hasMessage("Missing required field \"source-table\" in route configuration");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "flatten-array-structure"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("Route rules should be an array. Maybe missed a hyphen in YAML?");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "sink-missing"))
                .hasMessage("Missing required field \"sink-table\" in route configuration");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "table-delimiter-dangling"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "Dot (.) was used as delimiter between schema and table name, which should not present at the beginning. "
                                + "Other usages in RegExp should be escaped with backslash (\\).");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "table-delimiter-head"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "Dot (.) was used as delimiter between schema and table name, which should not present at the beginning. "
                                + "Other usages in RegExp should be escaped with backslash (\\).");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "table-delimiter-tail"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "Dot (.) was used as delimiter between schema and table name, which should not present at the end. "
                                + "Other usages in RegExp should be escaped with backslash (\\).");

        Assertions.assertThatCode(() -> verifyPipelineYaml("route", "table-delimiter-tail-legal"))
                .doesNotThrowAnyException();

        Assertions.assertThatThrownBy(
                        () -> verifyPipelineYaml("route", "table-delimiter-redundant"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "Dot (.) was used as delimiter between schema and table name. "
                                + "Other usages in RegExp should be escaped with backslash (\\).");

        Assertions.assertThatCode(
                        () -> verifyPipelineYaml("route", "table-delimiter-head-in-pattern"))
                .doesNotThrowAnyException();

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("route", "dangling-replace-symbol"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("Replace symbol is specified but not present in sink definition.");
    }

    @Test
    void testTransformGuard() {
        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("transform", "source-missing"))
                .hasMessage("Missing required field \"source-table\" in transform configuration");

        Assertions.assertThatThrownBy(
                        () -> verifyPipelineYaml("transform", "flatten-array-structure"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("Transform rules should be an array. Maybe missed a hyphen in YAML?");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("transform", "bad-primary-keys"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("1+1 is not a valid SQL identifier");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("transform", "bad-partition-keys"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("'colC' is not a valid SQL identifier");

        Assertions.assertThatThrownBy(
                        () -> verifyPipelineYaml("transform", "transform-not-identifier"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "123 is neither a column identifier nor an aliased expression. Expected: <Expression> AS <Identifier>");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("transform", "transform-forgot-as"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage(
                        "`FUN`() is neither a column identifier nor an aliased expression. Expected: <Expression> AS <Identifier>");

        Assertions.assertThatThrownBy(() -> verifyPipelineYaml("transform", "bad-filtering"))
                .isExactlyInstanceOf(GuardVerificationException.class)
                .hasMessage("a, b, c is not a valid filter rule");
    }
}
