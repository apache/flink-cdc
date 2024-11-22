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

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.runtime.model.ModelOptions.OPENAI_API_KEY;
import static org.apache.flink.cdc.runtime.model.ModelOptions.OPENAI_HOST;
import static org.apache.flink.cdc.runtime.model.ModelOptions.OPENAI_MODEL_NAME;

/**
 * A {@link BuiltInModel} that use Model defined by OpenAI to generate vector data, refer to <a
 * href="https://docs.langchain4j.dev/integrations/language-models/open-ai/">docs</a>}.
 */
public class OpenAIEmbeddingModel implements BuiltInModel, UserDefinedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(OpenAIEmbeddingModel.class);

    private String host;

    private String apiKey;

    private String modelName;

    private OpenAiEmbeddingModel embeddingModel;

    public void configure(Configuration modelOptions) {
        this.modelName = modelOptions.get(OPENAI_MODEL_NAME);
        if (modelName == null) {
            modelName = "text-embedding-ada-002";
        }
        this.host = modelOptions.get(OPENAI_HOST);
        this.apiKey = modelOptions.get(OPENAI_API_KEY);
    }

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
    public void open() throws Exception {
        LOG.info("Opening ModelUdf: {}", modelName);
        this.embeddingModel =
                OpenAiEmbeddingModel.builder()
                        .apiKey(apiKey)
                        .baseUrl(host)
                        .modelName(modelName)
                        .build();
    }
}
