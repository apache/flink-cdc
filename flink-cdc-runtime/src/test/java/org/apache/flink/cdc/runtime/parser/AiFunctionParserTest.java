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

import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsImageEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsImageTextGeneration;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.operators.transform.ProjectionColumn;
import org.apache.flink.cdc.runtime.operators.transform.UserDefinedFunctionDescriptor;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Parser and Janino tests for the generic AI functions. */
class AiFunctionParserTest {

    private static final List<Column> COLUMNS =
            List.of(
                    Column.physicalColumn("id", DataTypes.INT()),
                    Column.physicalColumn("content", DataTypes.STRING()),
                    Column.physicalColumn("image", DataTypes.BYTES()));

    private static class TextModelClient implements AiModelClient, SupportsTextGeneration {
        private static final long serialVersionUID = 1L;

        @Override
        public String generate(String systemPrompt, String userInput) {
            return "{}";
        }
    }

    private static class EmbeddingModelClient implements AiModelClient, SupportsEmbedding {
        private static final long serialVersionUID = 1L;

        @Override
        public float[] embed(String text) {
            return new float[0];
        }
    }

    private static class ImageTextModelClient
            implements AiModelClient, SupportsImageTextGeneration {
        private static final long serialVersionUID = 1L;

        @Override
        public String generateTextFromImage(byte[] image, String prompt) {
            return "description";
        }
    }

    private static class ImageEmbeddingModelClient
            implements AiModelClient, SupportsImageEmbedding {
        private static final long serialVersionUID = 1L;

        @Override
        public float[] embedImage(byte[] image) {
            return new float[0];
        }
    }

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
                        "aiClassify(model, $0, \"positive,negative\")",
                        "aiTranslate(model, $0, \"auto\", \"en\")",
                        "aiSummarize(model, $0, 100)",
                        "aiSentiment(model, $0)",
                        "aiExtract(model, $0, \"name:string\")",
                        "aiMask(model, $0, \"email,phone\")");
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
                        "aiImageComplete(vision, $0, \"Describe the image\")",
                        "aiImageEmbed(imageEmbedder, $0)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.FLOAT()));
    }

    @Test
    void testSameNamedUdfTakesPrecedenceOverAiFunction() {
        Set<String> udfNames = Set.of("ai_sentiment");
        assertThatCode(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_SENTIMENT(id) AS sentiment",
                                        null,
                                        Collections.emptySet(),
                                        udfNames))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_SENTIMENT(id) AS sentiment",
                                        null,
                                        Collections.emptyMap(),
                                        udfNames))
                .doesNotThrowAnyException();

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
        Set<String> udfNames = Set.of("ai_image_embed");

        assertThatCode(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_IMAGE_EMBED(id) AS embedding",
                                        null,
                                        Collections.emptySet(),
                                        udfNames))
                .doesNotThrowAnyException();
        assertThatCode(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_IMAGE_EMBED(id) AS embedding",
                                        null,
                                        Collections.emptyMap(),
                                        udfNames))
                .doesNotThrowAnyException();

        List<ProjectionColumn> columns =
                TransformParser.generateProjectionColumns(
                        "AI_IMAGE_EMBED(id) AS embedding",
                        COLUMNS,
                        List.of(
                                new UserDefinedFunctionDescriptor(
                                        "ai_image_embed",
                                        "org.apache.flink.cdc.udf.examples.java.AddOneFunctionClass")),
                        new SupportedMetadataColumn[0]);

        assertThat(columns)
                .extracting(ProjectionColumn::getScriptExpression)
                .containsExactly("__udf_ai_image_embed.eval($0)");
        assertThat(columns)
                .extracting(ProjectionColumn::getDataType)
                .containsExactly(DataTypes.STRING());
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

        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelReferences(
                                        "AI_CLASSIFY('missing', content, 'a,b') AS classified",
                                        null,
                                        Set.of("declared")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'missing'")
                .hasMessageContaining("AI_CLASSIFY");
    }

    @Test
    void testModelCapabilitiesMustMatchAiFunctions() {
        Map<String, AiModelClient> models =
                Map.of(
                        "textModel", new TextModelClient(),
                        "embeddingModel", new EmbeddingModelClient(),
                        "imageTextModel", new ImageTextModelClient(),
                        "imageEmbeddingModel", new ImageEmbeddingModelClient());

        assertThatCode(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_CLASSIFY('textModel', content, 'a,b') AS classified, "
                                                + "AI_EMBED('embeddingModel', content) AS embedding, "
                                                + "AI_IMAGE_COMPLETE('imageTextModel', image, 'describe') AS description, "
                                                + "AI_IMAGE_EMBED('imageEmbeddingModel', image) AS image_embedding",
                                        null,
                                        models))
                .doesNotThrowAnyException();
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_SENTIMENT('embeddingModel', content) AS sentiment",
                                        null,
                                        models))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'embeddingModel'")
                .hasMessageContaining("AI_SENTIMENT")
                .hasMessageContaining("does not support text generation");
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_EMBED('textModel', content) AS embedding",
                                        null,
                                        models))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'textModel'")
                .hasMessageContaining("AI_EMBED")
                .hasMessageContaining("does not support embedding");
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_IMAGE_COMPLETE('textModel', image, 'describe') AS description",
                                        null,
                                        models))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'textModel'")
                .hasMessageContaining("AI_IMAGE_COMPLETE")
                .hasMessageContaining("does not support image text generation");
        assertThatThrownBy(
                        () ->
                                TransformParser.validateAiModelCapabilities(
                                        "AI_IMAGE_EMBED('embeddingModel', image) AS embedding",
                                        null,
                                        models))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Model 'embeddingModel'")
                .hasMessageContaining("AI_IMAGE_EMBED")
                .hasMessageContaining("does not support image embedding");
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
