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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;

/** Parsed request and retry parameters for the OpenAI-compatible model client. */
class OpenAiRequestParams implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable final String userPrompt;
    @Nullable final Double temperature;
    @Nullable final Double topP;
    @Nullable final String stop;
    @Nullable final Integer maxTokens;
    @Nullable final Double presencePenalty;
    @Nullable final Double frequencyPenalty;
    @Nullable final Long n;
    @Nullable final Long seed;
    @Nullable final OpenAiCompatibleModelOptions.ResponseFormat responseFormat;
    final OpenAiCompatibleModelOptions.ContentType contentType;
    @Nullable final String extraHeader;
    @Nullable final String extraBody;
    @Nullable final Integer dimension;
    final ErrorHandlingStrategy errorHandlingStrategy;
    final int retryNum;
    final ErrorHandlingStrategy retryFallbackStrategy;
    final RetryBackoffStrategy retryBackoffStrategy;
    final long retryBackoffBaseIntervalMillis;

    private OpenAiRequestParams(
            @Nullable String userPrompt,
            @Nullable Double temperature,
            @Nullable Double topP,
            @Nullable String stop,
            @Nullable Integer maxTokens,
            @Nullable Double presencePenalty,
            @Nullable Double frequencyPenalty,
            @Nullable Long n,
            @Nullable Long seed,
            @Nullable OpenAiCompatibleModelOptions.ResponseFormat responseFormat,
            OpenAiCompatibleModelOptions.ContentType contentType,
            @Nullable String extraHeader,
            @Nullable String extraBody,
            @Nullable Integer dimension,
            ErrorHandlingStrategy errorHandlingStrategy,
            int retryNum,
            ErrorHandlingStrategy retryFallbackStrategy,
            RetryBackoffStrategy retryBackoffStrategy,
            long retryBackoffBaseIntervalMillis) {
        this.userPrompt = userPrompt;
        this.temperature = temperature;
        this.topP = topP;
        this.stop = stop;
        this.maxTokens = maxTokens;
        this.presencePenalty = presencePenalty;
        this.frequencyPenalty = frequencyPenalty;
        this.n = n;
        this.seed = seed;
        this.responseFormat = responseFormat;
        this.contentType = contentType;
        this.extraHeader = extraHeader;
        this.extraBody = extraBody;
        this.dimension = dimension;
        this.errorHandlingStrategy = errorHandlingStrategy;
        this.retryNum = retryNum;
        this.retryFallbackStrategy = retryFallbackStrategy;
        this.retryBackoffStrategy = retryBackoffStrategy;
        this.retryBackoffBaseIntervalMillis = retryBackoffBaseIntervalMillis;
    }

    static OpenAiRequestParams fromOptions(Configuration options) {
        String userPrompt = options.get(OpenAiCompatibleModelOptions.USER_PROMPT);
        Double temperature = options.get(OpenAiCompatibleModelOptions.TEMPERATURE);
        Double topP = options.get(OpenAiCompatibleModelOptions.TOP_P);
        String stop = options.get(OpenAiCompatibleModelOptions.STOP);
        Integer maxTokens = options.get(OpenAiCompatibleModelOptions.MAX_TOKENS);
        Double presencePenalty = options.get(OpenAiCompatibleModelOptions.PRESENCE_PENALTY);
        Double frequencyPenalty = options.get(OpenAiCompatibleModelOptions.FREQUENCY_PENALTY);
        Long n = options.get(OpenAiCompatibleModelOptions.N);
        Long seed = options.get(OpenAiCompatibleModelOptions.SEED);
        OpenAiCompatibleModelOptions.ResponseFormat responseFormat =
                options.get(OpenAiCompatibleModelOptions.RESPONSE_FORMAT);
        OpenAiCompatibleModelOptions.ContentType contentType =
                options.get(OpenAiCompatibleModelOptions.CONTENT_TYPE);
        String extraHeader = options.get(OpenAiCompatibleModelOptions.EXTRA_HEADER);
        String extraBody = options.get(OpenAiCompatibleModelOptions.EXTRA_BODY);
        Integer dimension = options.get(OpenAiCompatibleModelOptions.DIMENSION);
        ErrorHandlingStrategy errorHandlingStrategy =
                options.get(OpenAiCompatibleModelOptions.ERROR_HANDLING_STRATEGY);
        int retryNum = options.get(OpenAiCompatibleModelOptions.RETRY_NUM);
        ErrorHandlingStrategy retryFallbackStrategy =
                options.get(OpenAiCompatibleModelOptions.RETRY_FALLBACK_STRATEGY);
        RetryBackoffStrategy retryBackoffStrategy =
                options.get(OpenAiCompatibleModelOptions.RETRY_BACKOFF_STRATEGY);
        long retryBackoffBaseIntervalMillis =
                options.get(OpenAiCompatibleModelOptions.RETRY_BACKOFF_BASE_INTERVAL).toMillis();

        validateRange("temperature", temperature, 0.0d, 2.0d);
        validateRange("top-p", topP, 0.0d, 1.0d);
        validateRange("presence-penalty", presencePenalty, -2.0d, 2.0d);
        validateRange("frequency-penalty", frequencyPenalty, -2.0d, 2.0d);
        validatePositive("max-tokens", maxTokens);
        validatePositive("n", n);
        validatePositive("dimension", dimension);
        if (responseFormat == OpenAiCompatibleModelOptions.ResponseFormat.TEXT) {
            throw new IllegalArgumentException(
                    "Only 'json_object' is supported for option 'response-format', because the "
                            + "built-in AI completion function parses model output as JSON.");
        }
        if (retryNum < 1) {
            throw new IllegalArgumentException("Option 'retry-num' must be at least 1.");
        }
        if (retryFallbackStrategy == ErrorHandlingStrategy.RETRY) {
            throw new IllegalArgumentException("Option 'retry-fallback-strategy' cannot be retry.");
        }
        if (retryBackoffBaseIntervalMillis < 0) {
            throw new IllegalArgumentException(
                    "Option 'retry-backoff-base-interval' cannot be negative.");
        }
        validateRetryDelay(retryBackoffStrategy, retryBackoffBaseIntervalMillis, retryNum);

        return new OpenAiRequestParams(
                userPrompt,
                temperature,
                topP,
                stop,
                maxTokens,
                presencePenalty,
                frequencyPenalty,
                n,
                seed,
                responseFormat,
                contentType,
                extraHeader,
                extraBody,
                dimension,
                errorHandlingStrategy,
                retryNum,
                retryFallbackStrategy,
                retryBackoffStrategy,
                retryBackoffBaseIntervalMillis);
    }

    private static void validateRange(
            String option, @Nullable Double value, double minimum, double maximum) {
        if (value != null && (value < minimum || value > maximum)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Option '%s' must be between %s and %s.", option, minimum, maximum));
        }
    }

    private static void validatePositive(String option, @Nullable Number value) {
        if (value != null && value.longValue() < 1) {
            throw new IllegalArgumentException(
                    String.format("Option '%s' must be at least 1.", option));
        }
    }

    private static void validateRetryDelay(
            RetryBackoffStrategy strategy, long baseIntervalMillis, int retryNum) {
        try {
            strategy.minimumTotalDelay(baseIntervalMillis, retryNum);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Total retry delay is too large. Base interval: %s, strategy: %s, attempts: %s.",
                            Duration.ofMillis(baseIntervalMillis), strategy, retryNum),
                    e);
        }
    }
}
