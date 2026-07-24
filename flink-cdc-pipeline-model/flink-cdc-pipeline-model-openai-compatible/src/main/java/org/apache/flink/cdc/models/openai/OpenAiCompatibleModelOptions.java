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

package org.apache.flink.cdc.models.openai;

import org.apache.flink.cdc.common.configuration.ConfigOption;

import java.util.Set;

import static org.apache.flink.cdc.common.configuration.ConfigOptions.key;

/** Config options for {@link OpenAiCompatibleModelClient}. */
public class OpenAiCompatibleModelOptions {

    public static final ConfigOption<String> ENDPOINT =
            key("endpoint").stringType().noDefaultValue();

    public static final ConfigOption<String> API_KEY = key("api-key").stringType().noDefaultValue();

    public static final ConfigOption<String> MODEL_NAME =
            key("model-name").stringType().noDefaultValue();

    public static final ConfigOption<String> SYSTEM_PROMPT =
            key("system-prompt").stringType().noDefaultValue();

    public static final ConfigOption<Double> TEMPERATURE =
            key("temperature").doubleType().noDefaultValue();

    public static final ConfigOption<Double> TOP_P = key("top-p").doubleType().noDefaultValue();

    public static final ConfigOption<String> STOP = key("stop").stringType().noDefaultValue();

    public static final ConfigOption<Integer> MAX_TOKENS =
            key("max-tokens").intType().noDefaultValue();

    public static final ConfigOption<Double> PRESENCE_PENALTY =
            key("presence-penalty").doubleType().noDefaultValue();

    public static final ConfigOption<Integer> N = key("n").intType().noDefaultValue();

    public static final ConfigOption<Long> SEED = key("seed").longType().noDefaultValue();

    public static final ConfigOption<String> RESPONSE_FORMAT =
            key("response-format").stringType().defaultValue("text");

    public static final ConfigOption<String> EXTRA_HEADER =
            key("extra-header").stringType().noDefaultValue();

    public static final ConfigOption<String> EXTRA_BODY =
            key("extra-body").stringType().noDefaultValue();

    public static final ConfigOption<Integer> DIMENSION =
            key("dimension").intType().noDefaultValue();

    public static Set<ConfigOption<?>> requiredOptions() {
        return Set.of(ENDPOINT, API_KEY, MODEL_NAME);
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                SYSTEM_PROMPT,
                TEMPERATURE,
                TOP_P,
                STOP,
                MAX_TOKENS,
                PRESENCE_PENALTY,
                N,
                SEED,
                RESPONSE_FORMAT,
                EXTRA_HEADER,
                EXTRA_BODY,
                DIMENSION);
    }

    private OpenAiCompatibleModelOptions() {}
}
