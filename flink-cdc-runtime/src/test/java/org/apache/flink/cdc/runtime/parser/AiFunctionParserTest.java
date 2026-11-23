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
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parser and Janino tests for the generic AI functions. */
class AiFunctionParserTest {

    private static final List<Column> COLUMNS =
            List.of(
                    Column.physicalColumn("id", DataTypes.INT()),
                    Column.physicalColumn("content", DataTypes.STRING()),
                    Column.physicalColumn("image", DataTypes.BYTES()));

    @Test
    void testTranslateAiFunctions() {
        List<ProjectionColumn> columns =
                translate(
                        "AI_COMPLETE('completer', content, 'You are helpful') AS completed, "
                                + "AI_EMBED('embedder', content) AS embedding");

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly(
                        "aiComplete(\"completer\", $0, \"You are helpful\", __ai_model_clients__)",
                        "aiEmbed(\"embedder\", $0, __ai_model_clients__)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.VARIANT(), DataTypes.ARRAY(DataTypes.FLOAT()));
    }

    @Test
    void testTranslateSpecializedTextAiFunctions() {
        List<ProjectionColumn> columns =
                translate(
                        "AI_CLASSIFY('model', content, 'positive,negative') AS classified, "
                                + "AI_TRANSLATE('model', content, 'auto', 'en') AS translated, "
                                + "AI_SUMMARIZE('model', content, 100) AS summarized, "
                                + "AI_SENTIMENT('model', content) AS sentiment, "
                                + "AI_EXTRACT('model', content, 'name:string') AS extracted, "
                                + "AI_MASK('model', content, 'email,phone') AS masked");

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly(
                        "aiClassify(\"model\", $0, \"positive,negative\", __ai_model_clients__)",
                        "aiTranslate(\n"
                                + "    \"model\",\n"
                                + "    $0,\n"
                                + "    \"auto\",\n"
                                + "    \"en\",\n"
                                + "    __ai_model_clients__\n"
                                + ")",
                        "aiSummarize(\"model\", $0, 100, __ai_model_clients__)",
                        "aiSentiment(\"model\", $0, __ai_model_clients__)",
                        "aiExtract(\"model\", $0, \"name:string\", __ai_model_clients__)",
                        "aiMask(\"model\", $0, \"email,phone\", __ai_model_clients__)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsOnly(DataTypes.VARIANT());
    }

    @Test
    void testTranslateImageAiFunctions() {
        List<ProjectionColumn> columns =
                translate(
                        "AI_IMAGE_COMPLETE('vision', image, 'Describe the image') AS description, "
                                + "AI_IMAGE_EMBED('imageEmbedder', image) AS embedding");

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly(
                        "aiImageComplete(\"vision\", $0, \"Describe the image\", __ai_model_clients__)",
                        "aiImageEmbed(\"imageEmbedder\", $0, __ai_model_clients__)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.FLOAT()));
    }

    @Test
    void testDynamicModelSelection() {
        List<ProjectionColumn> columns =
                translate(
                        "AI_COMPLETE(IF(id = 1, 'powerful', 'cheap'), content, 'prompt') AS completed");

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly(
                        "aiComplete(isTrue(valueEquals($0, 1)) ? \"powerful\" : \"cheap\", $1, \"prompt\", __ai_model_clients__)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.VARIANT());
    }

    @Test
    void testSameNamedUdfTakesPrecedenceOverAiFunction() {
        List<ProjectionColumn> columns =
                TransformParser.generateProjectionColumns(
                        "AI_SENTIMENT(id) AS sentiment",
                        COLUMNS,
                        List.of(
                                new UserDefinedFunctionDescriptor(
                                        "ai_sentiment",
                                        "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass")),
                        new SupportedMetadataColumn[0]);

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly("__udf_ai_sentiment.eval($0)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.STRING());
    }

    @Test
    void testSameNamedUdfTakesPrecedenceOverImageAiFunction() {
        assertSameNamedUdfTakesPrecedenceOverImageAiFunction(
                "AI_IMAGE_COMPLETE", "ai_image_complete");
        assertSameNamedUdfTakesPrecedenceOverImageAiFunction("AI_IMAGE_EMBED", "ai_image_embed");
    }

    private static void assertSameNamedUdfTakesPrecedenceOverImageAiFunction(
            String functionName, String udfName) {
        String projection = functionName + "(id) AS udf_output";

        List<ProjectionColumn> columns =
                TransformParser.generateProjectionColumns(
                        projection,
                        COLUMNS,
                        List.of(
                                new UserDefinedFunctionDescriptor(
                                        udfName,
                                        "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass")),
                        new SupportedMetadataColumn[0]);

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly("__udf_" + udfName + ".eval($0)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.STRING());
    }

    @Test
    void testFunctionArityValidation() {
        assertThatThrownBy(() -> translate("AI_EMBED('model') AS embedding"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_EMBED'");
        assertThatThrownBy(() -> translate("AI_COMPLETE('model', content) AS completed"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_COMPLETE'");
        assertThatThrownBy(() -> translate("AI_COMPLETE() AS completed"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_COMPLETE'");
        assertThatThrownBy(() -> translate("AI_CLASSIFY('model', content) AS classified"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_CLASSIFY'");
        assertThatThrownBy(() -> translate("AI_TRANSLATE('model', content, 'auto') AS translated"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_TRANSLATE'");
        assertThatThrownBy(() -> translate("AI_SENTIMENT('model') AS sentiment"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_SENTIMENT'");
        assertThatThrownBy(() -> translate("AI_EXTRACT('model', content) AS extracted"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_EXTRACT'");
        assertThatThrownBy(() -> translate("AI_MASK('model', content) AS masked"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_MASK'");
        assertThatThrownBy(() -> translate("AI_SUMMARIZE('model', content, TRUE) AS summarized"))
                .hasMessageContaining("Cannot apply 'AI_SUMMARIZE'");
        assertThatThrownBy(() -> translate("AI_IMAGE_COMPLETE('model', image) AS description"))
                .hasMessageContaining(
                        "Invalid number of arguments to function 'AI_IMAGE_COMPLETE'");
        assertThatThrownBy(() -> translate("AI_IMAGE_EMBED('model') AS embedding"))
                .hasMessageContaining("Invalid number of arguments to function 'AI_IMAGE_EMBED'");
    }

    private List<ProjectionColumn> translate(String expression) {
        return TransformParser.generateProjectionColumns(
                expression, COLUMNS, Collections.emptyList(), new SupportedMetadataColumn[0]);
    }
}
