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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link TransformParser} and {@link JaninoCompiler} with AI functions. */
class AiFunctionParserTest {

    private static final List<Column> DUMMY_COLUMNS =
            List.of(
                    Column.physicalColumn("id", DataTypes.INT()),
                    Column.physicalColumn("content", DataTypes.STRING()),
                    Column.physicalColumn("text", DataTypes.STRING()),
                    Column.physicalColumn("flag", DataTypes.BOOLEAN()));
    private static final Map<String, String> COLUMN_MAP =
            Map.of(
                    "id", "$1",
                    "content", "$2",
                    "text", "$3",
                    "flag", "$4");

    @Test
    void testTranslateAiFunctionInProjection() {
        assertThat(
                        translateAsProjection(
                                "id, AI_COMPLETE('myModel', content, 'You are a classifier') AS output_col"))
                .map(ProjectionColumn::toString)
                .containsExactly(
                        "ProjectionColumn{column=`id` INT, expression='id', scriptExpression='$0', originalColumnNames=[id], columnNameMap={id=$0}}",
                        "ProjectionColumn{column=`output_col` VARIANT, expression='AI_COMPLETE('myModel', `TB`.`content`, 'You are a classifier')', scriptExpression='aiComplete(myModel, $0, \"You are a classifier\")', originalColumnNames=[content], columnNameMap={content=$0}}");

        assertThat(translateAsProjection("id, AI_SUMMARIZE('m1', content, 100) AS summary"))
                .map(ProjectionColumn::toString)
                .containsExactly(
                        "ProjectionColumn{column=`id` INT, expression='id', scriptExpression='$0', originalColumnNames=[id], columnNameMap={id=$0}}",
                        "ProjectionColumn{column=`summary` VARIANT, expression='AI_SUMMARIZE('m1', `TB`.`content`, 100)', scriptExpression='aiSummarize(m1, $0, 100)', originalColumnNames=[content], columnNameMap={content=$0}}");

        assertThatThrownBy(
                        () -> translateAsProjection("AI_SUMMARIZE('m', content, flag) AS out_col"))
                .hasMessageContaining(
                        "Cannot apply 'AI_SUMMARIZE' to arguments of type 'AI_SUMMARIZE(<CHAR(1)>, <VARCHAR(65536)>, <BOOLEAN>)'.")
                .hasMessageContaining(
                        "Supported form(s): 'AI_SUMMARIZE(<STRING>, <STRING>, <INTEGER>)'");

        assertThatThrownBy(() -> translateAsProjection("AI_EMBED('m') AS out_col"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_EMBED'.")
                .hasMessageContaining("Was expecting 2 arguments");

        assertThatThrownBy(() -> translateAsProjection("AI_COMPLETE('m', content) AS out_col"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_COMPLETE'.")
                .hasMessageContaining("Was expecting 3 arguments");
    }

    @Test
    void testAiFunctionRejectsNonStringLiteralModelArg() {
        assertThatThrownBy(
                        () ->
                                translateAsProjection(
                                        "AI_COMPLETE(content, content, 'prompt') AS out_col"))
                .hasMessageContaining(
                        "The first argument of AI function 'AI_COMPLETE' must be a string literal naming the model");

        assertThatThrownBy(() -> translateAsProjection("AI_EMBED(123, content) AS out_col"))
                .hasMessageContaining(
                        "The first argument of AI function 'AI_EMBED' must be a string literal naming the model");

        assertThatThrownBy(() -> translateAsProjection("AI_EMBED(UPPER('m'), content) AS out_col"))
                .hasMessageContaining(
                        "The first argument of AI function 'AI_EMBED' must be a string literal naming the model");

        assertThatThrownBy(
                        () ->
                                translateAsProjection(
                                        "AI_COMPLETE('my-model', content, 'p') AS out_col"))
                .hasMessageContaining(
                        "AI function model name 'my-model' is not a valid Java identifier.")
                .hasMessageContaining(
                        "Model names must follow Java identifier rules and must not be reserved keywords.");

        assertThatThrownBy(
                        () ->
                                translateAsProjection(
                                        "AI_COMPLETE('class', content, 'p') AS out_col"))
                .hasMessageContaining(
                        "AI function model name 'class' is not a valid Java identifier.")
                .hasMessageContaining(
                        "Model names must follow Java identifier rules and must not be reserved keywords.");
    }

    @Test
    void testTranslateAiFunctionInFilter() {
        assertThat(translateAsFilter("AI_COMPLETE('myModel', content, 'Classify this text')"))
                .isEqualTo("aiComplete(myModel, $2, \"Classify this text\")");
        assertThat(translateAsFilter("AI_SUMMARIZE('summarizer', content, 100)"))
                .isEqualTo("aiSummarize(summarizer, $2, 100)");
        assertThat(translateAsFilter("AI_EMBED('embedder', content)"))
                .isEqualTo("aiEmbed(embedder, $2)");
        assertThat(translateAsFilter("ai_complete('myModel', content, 'prompt')"))
                .isEqualTo("aiComplete(myModel, $2, \"prompt\")");
    }

    private List<ProjectionColumn> translateAsProjection(String expression) {
        return TransformParser.generateProjectionColumns(
                expression, DUMMY_COLUMNS, Collections.emptyList(), new SupportedMetadataColumn[0]);
    }

    private String translateAsFilter(String expression) {
        return TransformParser.translateFilterExpressionToJaninoExpression(
                expression,
                DUMMY_COLUMNS,
                Collections.emptyList(),
                new SupportedMetadataColumn[0],
                COLUMN_MAP);
    }
}
