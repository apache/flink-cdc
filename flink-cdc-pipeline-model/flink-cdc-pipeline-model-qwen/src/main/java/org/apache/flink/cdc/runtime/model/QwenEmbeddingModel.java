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

package org.apache.flink.cdc.runtime.model;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.Preconditions;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.runtime.model.ModelOptions.QWEN_API_KEY;
import static org.apache.flink.cdc.runtime.model.ModelOptions.QWEN_MODEL_NAME;

/**
 * A {@link UserDefinedFunction} that use Model defined by Qwen to generate vector data, refer to <a
 * href="https://docs.langchain4j.dev/integrations/embedding-models/dashscope">docs</a>}.
 */
public class QwenEmbeddingModel implements UserDefinedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(QwenEmbeddingModel.class);

    private String modelName;

    private dev.langchain4j.model.dashscope.QwenEmbeddingModel embeddingModel;

    public ArrayData eval(String input) {
        return getEmbedding(input);
    }

    private ArrayData getEmbedding(String input) {
        if (input == null || input.trim().isEmpty()) {
            LOG.debug("Empty or null input provided for embedding.");
            return new GenericArrayData(new Float[0]);
        }

        TextSegment textSegment = new TextSegment(input, new Metadata());

        List<Embedding> embeddings =
                embeddingModel.embedAll(Collections.singletonList(textSegment)).content();

        if (embeddings != null && !embeddings.isEmpty()) {
            List<Float> embeddingList = embeddings.get(0).vectorAsList();
            Float[] embeddingArray = embeddingList.toArray(new Float[0]);
            return new GenericArrayData(embeddingArray);
        } else {
            LOG.warn("No embedding results returned for input: {}", input);
            return new GenericArrayData(new Float[0]);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.ARRAY(DataTypes.FLOAT());
    }

    @Override
    public void open(UserDefinedFunctionContext userDefinedFunctionContext) {
        Configuration modelOptions = userDefinedFunctionContext.configuration();
        this.modelName = modelOptions.get(QWEN_MODEL_NAME);
        Preconditions.checkNotNull(modelName, QWEN_MODEL_NAME.key() + " should not be empty.");
        String apiKey = modelOptions.get(QWEN_API_KEY);
        Preconditions.checkNotNull(apiKey, QWEN_API_KEY.key() + " should not be empty.");
        LOG.info("Opening QwenEmbeddingModel " + modelName);
        this.embeddingModel =
                dev.langchain4j.model.dashscope.QwenEmbeddingModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelName)
                        .build();
    }

    @Override
    public void close() {
        LOG.info("Closed OpenAIEmbeddingModel " + modelName);
    }
}
