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
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.common.utils.Preconditions;

import dev.langchain4j.data.message.UserMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.flink.cdc.runtime.model.ModelOptions.QWEN_API_KEY;
import static org.apache.flink.cdc.runtime.model.ModelOptions.QWEN_CHAT_PROMPT;
import static org.apache.flink.cdc.runtime.model.ModelOptions.QWEN_MODEL_NAME;

/**
 * A {@link UserDefinedFunction} that use Model defined by Qwen to generate text, refer to <a
 * href="https://docs.langchain4j.dev/integrations/language-models/dashscope">docs</a>}.
 */
public class QwenChatModel implements UserDefinedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(QwenChatModel.class);

    private dev.langchain4j.model.dashscope.QwenChatModel chatModel;

    private String modelName;

    private String prompt;

    public String eval(String input) {
        return chat(input);
    }

    private String chat(String input) {
        if (input == null || input.trim().isEmpty()) {
            LOG.warn("Empty or null input provided for embedding.");
            return "";
        }
        if (prompt != null) {
            input = prompt + ": " + input;
        }
        return chatModel
                .generate(Collections.singletonList(new UserMessage(input)))
                .content()
                .text();
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.STRING();
    }

    @Override
    public void open(UserDefinedFunctionContext userDefinedFunctionContext) {
        Configuration modelOptions = userDefinedFunctionContext.configuration();
        this.modelName = modelOptions.get(QWEN_MODEL_NAME);
        Preconditions.checkNotNull(modelName, QWEN_MODEL_NAME.key() + " should not be empty.");
        String apiKey = modelOptions.get(QWEN_API_KEY);
        Preconditions.checkNotNull(apiKey, QWEN_API_KEY.key() + " should not be empty.");
        this.prompt = modelOptions.get(QWEN_CHAT_PROMPT);
        this.chatModel =
                dev.langchain4j.model.dashscope.QwenChatModel.builder()
                        .apiKey(apiKey)
                        .modelName(modelName)
                        .build();
    }

    @Override
    public String toString() {
        return "QwenChatModel{"
                + "chatModel="
                + chatModel
                + ", modelName='"
                + modelName
                + '\''
                + ", prompt='"
                + prompt
                + '\''
                + '}';
    }

    @Override
    public void close() {
        LOG.info("Closed OpenAIChatModel " + modelName);
    }
}
