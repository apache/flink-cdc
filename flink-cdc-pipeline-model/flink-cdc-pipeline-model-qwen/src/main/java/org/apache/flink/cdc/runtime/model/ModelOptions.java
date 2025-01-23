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

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Options of built-in qwen model. */
public class ModelOptions {

    // Options for Qwen Model.
    public static final ConfigOption<String> QWEN_MODEL_NAME =
            ConfigOptions.key("qwen.model")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of model to be called.");

    public static final ConfigOption<String> QWEN_API_KEY =
            ConfigOptions.key("qwen.apikey")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Api Key for verification of the Model server.");

    public static final ConfigOption<String> QWEN_CHAT_PROMPT =
            ConfigOptions.key("qwen.chat.prompt")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Prompt for chat using OpenAI.");
}
