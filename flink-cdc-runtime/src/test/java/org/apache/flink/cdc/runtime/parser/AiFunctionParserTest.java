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

package org.apache.flink.cdc.runtime.parser;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parser and Janino tests for the generic AI functions. */
class AiFunctionParserTest {

    private static final List<Column> COLUMNS =
            List.of(
                    Column.physicalColumn("id", DataTypes.INT()),
                    Column.physicalColumn("content", DataTypes.STRING()));

    @Test
    void testTranslateAiFunctions() {
        List<ProjectionColumn> columns =
                translate(
                        "AI_COMPLETE('completer', content, 'You are helpful') AS completed, "
                                + "AI_EMBED('embedder', content) AS embedding");

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly(
                        "aiComplete(completer, $0, \"You are helpful\")", "aiEmbed(embedder, $0)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.VARIANT(), DataTypes.ARRAY(DataTypes.FLOAT()));
    }

    @Test
    void testModelArgumentMustBeStringConstant() {
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_COMPLETE(content, content, 'prompt') AS completed",
                                        null,
                                        Set.of("content")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be a string constant");
    }

    @Test
    void testReferencedModelMustBeDeclared() {
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_EMBED('missing', content) AS embedding",
                                        null,
                                        Set.of("declared")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'missing'")
                .hasMessageContaining("has not been declared");

        assertThatCode(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_EMBED('declared', content) AS embedding",
                                        null,
                                        Set.of("declared")))
                .doesNotThrowAnyException();
    }

    @Test
    void testFunctionArityValidation() {
        assertThatThrownBy(() -> translate("AI_EMBED('model') AS embedding"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_EMBED'");
        assertThatThrownBy(() -> translate("AI_COMPLETE('model', content) AS completed"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_COMPLETE'");
        assertThatThrownBy(() -> translate("AI_COMPLETE() AS completed"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_COMPLETE'");
        assertThatCode(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_COMPLETE() AS completed", null, Collections.emptySet()))
                .doesNotThrowAnyException();
    }

    private List<ProjectionColumn> translate(String expression) {
        return TransformParser.generateProjectionColumns(
                expression, COLUMNS, Collections.emptyList(), new SupportedMetadataColumn[0]);
    }
}
