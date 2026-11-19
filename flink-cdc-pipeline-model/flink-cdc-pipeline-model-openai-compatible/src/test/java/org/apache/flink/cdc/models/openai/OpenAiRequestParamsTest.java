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

import org.apache.flink.cdc.common.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link OpenAiRequestParams}. */
class OpenAiRequestParamsTest {

    @Test
    void testParseRequestAndRetryOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("user-prompt", "follow up");
        options.put("temperature", "0.5");
        options.put("top-p", "0.7");
        options.put("stop", "END");
        options.put("max-tokens", "256");
        options.put("presence-penalty", "1.2");
        options.put("frequency-penalty", "0.4");
        options.put("n", "2");
        options.put("seed", "123");
        options.put("response-format", "json_object");
        options.put("content-type", "image_url");
        options.put("extra-header", "{\"x-test\":\"value\"}");
        options.put("extra-body", "{\"vendor\":true}");
        options.put("dimension", "768");
        options.put("error-handling-strategy", "retry");
        options.put("retry-num", "3");
        options.put("retry-fallback-strategy", "ignore");
        options.put("retry-backoff-strategy", "exponential");
        options.put("retry-backoff-base-interval", "2 s");

        OpenAiRequestParams params =
                OpenAiRequestParams.fromOptions(Configuration.fromMap(options));

        assertThat(params.userPrompt).isEqualTo("follow up");
        assertThat(params.temperature).isEqualTo(0.5d);
        assertThat(params.topP).isEqualTo(0.7d);
        assertThat(params.stop).isEqualTo("END");
        assertThat(params.maxTokens).isEqualTo(256);
        assertThat(params.presencePenalty).isEqualTo(1.2d);
        assertThat(params.frequencyPenalty).isEqualTo(0.4d);
        assertThat(params.n).isEqualTo(2L);
        assertThat(params.seed).isEqualTo(123L);
        assertThat(params.responseFormat)
                .isEqualTo(OpenAiCompatibleModelOptions.ResponseFormat.JSON_OBJECT);
        assertThat(params.contentType)
                .isEqualTo(OpenAiCompatibleModelOptions.ContentType.IMAGE_URL);
        assertThat(params.extraHeader).contains("x-test");
        assertThat(params.extraBody).contains("vendor");
        assertThat(params.dimension).isEqualTo(768);
        assertThat(params.errorHandlingStrategy).isEqualTo(ErrorHandlingStrategy.RETRY);
        assertThat(params.retryNum).isEqualTo(3);
        assertThat(params.retryFallbackStrategy).isEqualTo(ErrorHandlingStrategy.IGNORE);
        assertThat(params.retryBackoffStrategy).isEqualTo(RetryBackoffStrategy.EXPONENTIAL);
        assertThat(params.retryBackoffBaseIntervalMillis).isEqualTo(2000L);
    }

    @Test
    void testRetryDefaults() {
        OpenAiRequestParams params =
                OpenAiRequestParams.fromOptions(Configuration.fromMap(new HashMap<>()));

        assertThat(params.contentType).isEqualTo(OpenAiCompatibleModelOptions.ContentType.TEXT);
        assertThat(params.errorHandlingStrategy).isEqualTo(ErrorHandlingStrategy.RETRY);
        assertThat(params.retryNum).isEqualTo(100);
        assertThat(params.retryFallbackStrategy).isEqualTo(ErrorHandlingStrategy.FAILOVER);
        assertThat(params.retryBackoffStrategy).isEqualTo(RetryBackoffStrategy.FIXED);
        assertThat(params.retryBackoffBaseIntervalMillis).isEqualTo(1000L);
    }

    @Test
    void testInvalidParameterRangesAreRejected() {
        assertThatThrownBy(() -> parseOption("temperature", "2.1"))
                .hasMessageContaining("temperature");
        assertThatThrownBy(() -> parseOption("top-p", "-0.1")).hasMessageContaining("top-p");
        assertThatThrownBy(() -> parseOption("dimension", "0")).hasMessageContaining("dimension");
        assertThatThrownBy(() -> parseOption("retry-num", "0")).hasMessageContaining("retry-num");
    }

    @Test
    void testInvalidRetrySettingsAreRejected() {
        assertThatThrownBy(() -> parseOption("retry-fallback-strategy", "retry"))
                .hasMessageContaining("cannot be retry");

        Map<String, String> options = new HashMap<>();
        options.put("retry-backoff-strategy", "exponential");
        options.put("retry-num", "100");
        assertThatThrownBy(() -> OpenAiRequestParams.fromOptions(Configuration.fromMap(options)))
                .hasMessageContaining("Total retry delay is too large");
    }

    @Test
    void testTextResponseFormatIsRejected() {
        assertThatThrownBy(() -> parseOption("response-format", "text"))
                .hasMessageContaining("Only 'json_object' is supported");
    }

    private static void parseOption(String key, String value) {
        OpenAiRequestParams.fromOptions(Configuration.fromMap(Map.of(key, value)));
    }
}
