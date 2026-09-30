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
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import com.openai.models.ResponseFormatJsonObject;
import com.openai.models.ResponseFormatText;
import com.openai.models.chat.completions.ChatCompletionCreateParams;

import java.time.Duration;
import java.util.Set;

/** Config options accepted by the {@code openai-compatible} pipeline model. */
public class OpenAiCompatibleModelOptions {

    public static final ConfigOption<String> MODEL =
            ConfigOptions.key("model")
                    .stringType()
                    .noDefaultValue()
                    .withDeprecatedKeys("model-name")
                    .withDescription("Name of the model to invoke.");

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Base URL of the OpenAI-compatible endpoint.");

    public static final ConfigOption<String> API_KEY =
            ConfigOptions.key("api-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API key used to authenticate against the endpoint.");

    public static final ConfigOption<String> SYSTEM_PROMPT =
            ConfigOptions.key("system-prompt")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("System prompt prepended to every text completion request.");

    public static final ConfigOption<String> USER_PROMPT =
            ConfigOptions.key("user-prompt")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Additional user prompt appended after the input.");

    public static final ConfigOption<Double> TEMPERATURE =
            ConfigOptions.key("temperature")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Sampling temperature.");

    public static final ConfigOption<Double> TOP_P =
            ConfigOptions.key("top-p")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Nucleus sampling probability mass.");

    public static final ConfigOption<String> STOP =
            ConfigOptions.key("stop")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Stop sequence that ends generation.");

    public static final ConfigOption<Integer> MAX_TOKENS =
            ConfigOptions.key("max-tokens")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Maximum number of tokens to generate.");

    public static final ConfigOption<Double> PRESENCE_PENALTY =
            ConfigOptions.key("presence-penalty")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Presence penalty applied during generation.");

    public static final ConfigOption<Double> FREQUENCY_PENALTY =
            ConfigOptions.key("frequency-penalty")
                    .doubleType()
                    .noDefaultValue()
                    .withDescription("Frequency penalty applied during generation.");

    public static final ConfigOption<Long> N =
            ConfigOptions.key("n")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Number of chat completion choices to generate.");

    public static final ConfigOption<Long> SEED =
            ConfigOptions.key("seed")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Seed for deterministic sampling.");

    public static final ConfigOption<ResponseFormat> RESPONSE_FORMAT =
            ConfigOptions.key("response-format")
                    .enumType(ResponseFormat.class)
                    .noDefaultValue()
                    .withDescription("Response format of the chat completion.");

    public static final ConfigOption<ContentType> CONTENT_TYPE =
            ConfigOptions.key("content-type")
                    .enumType(ContentType.class)
                    .defaultValue(ContentType.TEXT)
                    .withDescription("Content type of the model input.");

    public static final ConfigOption<String> EXTRA_HEADER =
            ConfigOptions.key("extra-header")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Additional HTTP headers as a JSON object.");

    public static final ConfigOption<String> EXTRA_BODY =
            ConfigOptions.key("extra-body")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Additional request-body properties as a JSON object.");

    public static final ConfigOption<Integer> DIMENSION =
            ConfigOptions.key("dimension")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Number of dimensions for the embedding output.");

    public static final ConfigOption<ErrorHandlingStrategy> ERROR_HANDLING_STRATEGY =
            ConfigOptions.key("error-handling-strategy")
                    .enumType(ErrorHandlingStrategy.class)
                    .defaultValue(ErrorHandlingStrategy.RETRY)
                    .withDescription("Strategy applied when a request fails.");

    public static final ConfigOption<Integer> RETRY_NUM =
            ConfigOptions.key("retry-num")
                    .intType()
                    .defaultValue(100)
                    .withDescription("Maximum number of attempts when retry is enabled.");

    public static final ConfigOption<ErrorHandlingStrategy> RETRY_FALLBACK_STRATEGY =
            ConfigOptions.key("retry-fallback-strategy")
                    .enumType(ErrorHandlingStrategy.class)
                    .defaultValue(ErrorHandlingStrategy.FAILOVER)
                    .withDescription("Strategy applied after retries are exhausted.");

    public static final ConfigOption<RetryBackoffStrategy> RETRY_BACKOFF_STRATEGY =
            ConfigOptions.key("retry-backoff-strategy")
                    .enumType(RetryBackoffStrategy.class)
                    .defaultValue(RetryBackoffStrategy.FIXED)
                    .withDescription("Backoff strategy between retry attempts.");

    public static final ConfigOption<Duration> RETRY_BACKOFF_BASE_INTERVAL =
            ConfigOptions.key("retry-backoff-base-interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("Base interval between retry attempts.");

    public static final Set<ConfigOption<?>> ALL_OPTIONS =
            Set.of(
                    MODEL,
                    ENDPOINT,
                    API_KEY,
                    SYSTEM_PROMPT,
                    USER_PROMPT,
                    TEMPERATURE,
                    TOP_P,
                    STOP,
                    MAX_TOKENS,
                    PRESENCE_PENALTY,
                    FREQUENCY_PENALTY,
                    N,
                    SEED,
                    RESPONSE_FORMAT,
                    CONTENT_TYPE,
                    EXTRA_HEADER,
                    EXTRA_BODY,
                    DIMENSION,
                    ERROR_HANDLING_STRATEGY,
                    RETRY_NUM,
                    RETRY_FALLBACK_STRATEGY,
                    RETRY_BACKOFF_STRATEGY,
                    RETRY_BACKOFF_BASE_INTERVAL);

    /** Format of a text completion response. */
    public enum ResponseFormat {
        TEXT {
            @Override
            ChatCompletionCreateParams.ResponseFormat toResponseFormat() {
                return ChatCompletionCreateParams.ResponseFormat.ofText(
                        ResponseFormatText.builder().build());
            }
        },
        JSON_OBJECT {
            @Override
            ChatCompletionCreateParams.ResponseFormat toResponseFormat() {
                return ChatCompletionCreateParams.ResponseFormat.ofJsonObject(
                        ResponseFormatJsonObject.builder().build());
            }
        };

        abstract ChatCompletionCreateParams.ResponseFormat toResponseFormat();
    }

    /** Content type of the user input. */
    public enum ContentType {
        TEXT,
        IMAGE_URL
    }

    private OpenAiCompatibleModelOptions() {}
}
