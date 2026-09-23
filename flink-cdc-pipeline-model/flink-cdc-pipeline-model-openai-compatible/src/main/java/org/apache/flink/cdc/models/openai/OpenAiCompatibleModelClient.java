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

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.model.AiModelClient;
import org.apache.flink.cdc.common.model.abilities.SupportsEmbedding;
import org.apache.flink.cdc.common.model.abilities.SupportsImageTextGeneration;
import org.apache.flink.cdc.common.model.abilities.SupportsTextGeneration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.core.JsonValue;
import com.openai.errors.OpenAIIoException;
import com.openai.errors.OpenAIRetryableException;
import com.openai.errors.OpenAIServiceException;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionContentPart;
import com.openai.models.chat.completions.ChatCompletionContentPartImage;
import com.openai.models.chat.completions.ChatCompletionContentPartText;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.embeddings.CreateEmbeddingResponse;
import com.openai.models.embeddings.Embedding;
import com.openai.models.embeddings.EmbeddingCreateParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** AI model client that connects to an OpenAI-compatible endpoint. */
public class OpenAiCompatibleModelClient
        implements AiModelClient,
                SupportsTextGeneration,
                SupportsEmbedding,
                SupportsImageTextGeneration {

    private static final Logger LOG = LoggerFactory.getLogger(OpenAiCompatibleModelClient.class);

    private static final byte[] PNG_SIGNATURE =
            new byte[] {(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A};
    private static final byte[] JPEG_SIGNATURE = new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF};
    private static final byte[] GIF87A_SIGNATURE = new byte[] {0x47, 0x49, 0x46, 0x38, 0x37, 0x61};
    private static final byte[] GIF89A_SIGNATURE = new byte[] {0x47, 0x49, 0x46, 0x38, 0x39, 0x61};
    private static final byte[] RIFF_SIGNATURE = new byte[] {0x52, 0x49, 0x46, 0x46};
    private static final byte[] WEBP_SIGNATURE = new byte[] {0x57, 0x45, 0x42, 0x50};
    private static final int WEBP_SIGNATURE_OFFSET = 8;

    private static final long serialVersionUID = 1L;

    private final String endpoint;
    private final String apiKey;
    private final String model;
    @Nullable private final String configuredSystemPrompt;
    private final OpenAiRequestParams params;

    private transient OpenAIClient client;
    private transient Map<String, List<String>> additionalHeaders;
    private transient Map<String, JsonValue> additionalBody;

    OpenAiCompatibleModelClient(
            String endpoint,
            String apiKey,
            String model,
            @Nullable String configuredSystemPrompt,
            OpenAiRequestParams params) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.model = model;
        this.configuredSystemPrompt = configuredSystemPrompt;
        this.params = params;
    }

    @Override
    public void open() {
        additionalHeaders = parseHeaders(params.extraHeader);
        additionalBody = parseBody(params.extraBody);
        client =
                OpenAIOkHttpClient.builder().baseUrl(endpoint).apiKey(apiKey).maxRetries(0).build();
        LOG.info("Opened OpenAI-compatible model client. Endpoint: {} Model: {}", endpoint, model);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public String generate(String systemPrompt, String userInput) {
        return executeWithRetry("text completion", () -> complete(systemPrompt, userInput));
    }

    @Override
    public float[] embed(String text) {
        return executeWithRetry("embedding", () -> createEmbedding(text));
    }

    @Override
    public String generateTextFromImage(byte[] image, String prompt) {
        if (image == null) {
            return null;
        }
        String imageDataUrl = buildImageDataUrl(image);
        return executeWithRetry("image completion", () -> completeImage(imageDataUrl, prompt));
    }

    private String complete(String systemPrompt, String userInput) {
        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder().model(model);
        String effectiveSystemPrompt =
                configuredSystemPrompt == null
                        ? systemPrompt
                        : configuredSystemPrompt + "\n" + systemPrompt;
        builder.addSystemMessage(effectiveSystemPrompt);
        if (params.contentType == OpenAiCompatibleModelOptions.ContentType.IMAGE_URL) {
            ChatCompletionContentPartImage.ImageUrl imageUrl =
                    ChatCompletionContentPartImage.ImageUrl.builder().url(userInput).build();
            ChatCompletionContentPartImage image =
                    ChatCompletionContentPartImage.builder().imageUrl(imageUrl).build();
            builder.addUserMessageOfArrayOfContentParts(
                    Collections.singletonList(ChatCompletionContentPart.ofImageUrl(image)));
        } else {
            builder.addUserMessage(userInput);
        }
        if (params.userPrompt != null) {
            builder.addUserMessage(params.userPrompt);
        }
        applyCompletionParams(builder);
        builder.putAllAdditionalHeaders(headersOrEmpty());
        builder.putAllAdditionalBodyProperties(bodyOrEmpty());

        ChatCompletion completion = currentClient().chat().completions().create(builder.build());
        if (completion.choices().isEmpty()) {
            throw new IllegalStateException(
                    "OpenAI-compatible text completion returned no choices.");
        }
        return completion
                .choices()
                .get(0)
                .message()
                .content()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "OpenAI-compatible text completion returned no text content."));
    }

    private String completeImage(String imageDataUrl, String prompt) {
        ChatCompletionContentPart imagePart =
                ChatCompletionContentPart.ofImageUrl(
                        ChatCompletionContentPartImage.builder()
                                .imageUrl(
                                        ChatCompletionContentPartImage.ImageUrl.builder()
                                                .url(imageDataUrl)
                                                .build())
                                .build());
        ChatCompletionContentPart textPart =
                ChatCompletionContentPart.ofText(
                        ChatCompletionContentPartText.builder()
                                .text(prompt != null ? prompt : "")
                                .build());

        ChatCompletionCreateParams.Builder builder =
                ChatCompletionCreateParams.builder().model(model);
        if (configuredSystemPrompt != null) {
            builder.addSystemMessage(configuredSystemPrompt);
        }
        builder.addUserMessageOfArrayOfContentParts(List.of(textPart, imagePart));
        if (params.userPrompt != null) {
            builder.addUserMessage(params.userPrompt);
        }
        applyCompletionParams(builder);
        builder.putAllAdditionalHeaders(headersOrEmpty());
        builder.putAllAdditionalBodyProperties(bodyOrEmpty());

        ChatCompletion completion = currentClient().chat().completions().create(builder.build());
        if (completion.choices().isEmpty()) {
            throw new IllegalStateException(
                    "OpenAI-compatible image completion returned no choices.");
        }
        return completion
                .choices()
                .get(0)
                .message()
                .content()
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "OpenAI-compatible image completion returned no text content."));
    }

    private static String buildImageDataUrl(byte[] image) {
        return "data:"
                + detectImageMimeType(image)
                + ";base64,"
                + Base64.getEncoder().encodeToString(image);
    }

    /**
     * Detects the MIME type of an image from its leading magic bytes. Throws {@link
     * IllegalArgumentException} when the format cannot be recognized or the input is empty.
     */
    @VisibleForTesting
    static String detectImageMimeType(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Image bytes must not be null or empty.");
        }
        if (matchesSignature(bytes, 0, PNG_SIGNATURE)) {
            return "image/png";
        }
        if (matchesSignature(bytes, 0, JPEG_SIGNATURE)) {
            return "image/jpeg";
        }
        if (matchesSignature(bytes, 0, GIF87A_SIGNATURE)
                || matchesSignature(bytes, 0, GIF89A_SIGNATURE)) {
            return "image/gif";
        }
        if (matchesSignature(bytes, 0, RIFF_SIGNATURE)
                && matchesSignature(bytes, WEBP_SIGNATURE_OFFSET, WEBP_SIGNATURE)) {
            return "image/webp";
        }
        throw new IllegalArgumentException(
                "Unrecognized image format. Supported formats: PNG, JPEG, GIF, WebP.");
    }

    private static boolean matchesSignature(byte[] bytes, int offset, byte[] signature) {
        if (bytes.length < offset + signature.length) {
            return false;
        }
        for (int i = 0; i < signature.length; i++) {
            if (bytes[offset + i] != signature[i]) {
                return false;
            }
        }
        return true;
    }

    private void applyCompletionParams(ChatCompletionCreateParams.Builder builder) {
        if (params.temperature != null) {
            builder.temperature(params.temperature);
        }
        if (params.topP != null) {
            builder.topP(params.topP);
        }
        if (params.stop != null) {
            builder.stop(params.stop);
        }
        if (params.maxTokens != null) {
            builder.maxTokens(params.maxTokens);
        }
        if (params.presencePenalty != null) {
            builder.presencePenalty(params.presencePenalty);
        }
        if (params.frequencyPenalty != null) {
            builder.frequencyPenalty(params.frequencyPenalty);
        }
        if (params.n != null) {
            builder.n(params.n);
        }
        if (params.seed != null) {
            builder.seed(params.seed);
        }
        if (params.responseFormat != null) {
            builder.responseFormat(params.responseFormat.toResponseFormat());
        }
    }

    private float[] createEmbedding(String text) {
        EmbeddingCreateParams.Builder builder =
                EmbeddingCreateParams.builder().model(model).input(text);
        if (params.dimension != null) {
            builder.dimensions(params.dimension);
        }
        builder.putAllAdditionalHeaders(headersOrEmpty());
        builder.putAllAdditionalBodyProperties(bodyOrEmpty());

        CreateEmbeddingResponse response = currentClient().embeddings().create(builder.build());
        List<Embedding> data = response.data();
        if (data.isEmpty()) {
            return new float[0];
        }
        List<Float> embedding = data.get(0).embedding();
        float[] result = new float[embedding.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = embedding.get(i);
        }
        return result;
    }

    private <T> T executeWithRetry(String operation, Supplier<T> action) {
        int maximumAttempts =
                params.errorHandlingStrategy == ErrorHandlingStrategy.RETRY ? params.retryNum : 1;
        long intervalMillis = params.retryBackoffBaseIntervalMillis;
        RuntimeException lastException = null;
        int attempts = 0;
        for (; attempts < maximumAttempts; attempts++) {
            try {
                return action.get();
            } catch (RuntimeException e) {
                lastException = e;
                boolean hasAnotherAttempt = attempts + 1 < maximumAttempts;
                if (!hasAnotherAttempt || !isRetryable(e)) {
                    attempts++;
                    break;
                }
                LOG.warn(
                        "OpenAI-compatible {} request failed on attempt {}. Retrying in {} ms. Cause: {}",
                        operation,
                        attempts + 1,
                        intervalMillis,
                        e.toString());
                sleepBeforeRetry(intervalMillis);
                intervalMillis = params.retryBackoffStrategy.nextInterval(intervalMillis);
            }
        }

        ErrorHandlingStrategy finalStrategy =
                params.errorHandlingStrategy == ErrorHandlingStrategy.RETRY
                        ? params.retryFallbackStrategy
                        : params.errorHandlingStrategy;
        if (finalStrategy == ErrorHandlingStrategy.IGNORE) {
            LOG.warn(
                    "OpenAI-compatible {} request failed after {} attempt(s). Ignoring the input. Cause: {}",
                    operation,
                    attempts,
                    lastException);
            return null;
        }
        throw new RuntimeException(
                String.format(
                        "OpenAI-compatible %s request failed after %s attempt(s).",
                        operation, attempts),
                lastException);
    }

    private static void sleepBeforeRetry(long intervalMillis) {
        try {
            Thread.sleep(intervalMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while backing off an OpenAI request.", e);
        }
    }

    private static boolean isRetryable(RuntimeException exception) {
        if (hasThrowable(exception, IOException.class)
                || hasThrowable(exception, OpenAIIoException.class)
                || hasThrowable(exception, OpenAIRetryableException.class)) {
            return true;
        }
        for (Throwable throwable = exception; throwable != null; throwable = throwable.getCause()) {
            if (throwable instanceof OpenAIServiceException) {
                int statusCode = ((OpenAIServiceException) throwable).statusCode();
                return statusCode == 408
                        || statusCode == 409
                        || statusCode == 429
                        || (statusCode >= 500 && statusCode < 600);
            }
        }
        return false;
    }

    private OpenAIClient currentClient() {
        if (client == null) {
            throw new IllegalStateException("OpenAI-compatible model client has not been opened.");
        }
        return client;
    }

    private Map<String, List<String>> headersOrEmpty() {
        return additionalHeaders == null ? Collections.emptyMap() : additionalHeaders;
    }

    private Map<String, JsonValue> bodyOrEmpty() {
        return additionalBody == null ? Collections.emptyMap() : additionalBody;
    }

    private static Map<String, List<String>> parseHeaders(@Nullable String headerJson) {
        if (headerJson == null || headerJson.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        JsonNode root = parseJsonObject(headerJson, "extra-header");
        Map<String, List<String>> headers = new HashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> fields = root.fields(); fields.hasNext(); ) {
            Map.Entry<String, JsonNode> field = fields.next();
            List<String> values = new ArrayList<>();
            if (field.getValue().isArray()) {
                for (JsonNode value : field.getValue()) {
                    values.add(headerValue(field.getKey(), value));
                }
            } else {
                values.add(headerValue(field.getKey(), field.getValue()));
            }
            headers.put(field.getKey(), values);
        }
        return headers;
    }

    private static String headerValue(String name, JsonNode value) {
        if (!value.isValueNode() || value.isNull()) {
            throw new IllegalArgumentException(
                    String.format("Header '%s' must contain a scalar JSON value.", name));
        }
        return value.asText();
    }

    private static Map<String, JsonValue> parseBody(@Nullable String bodyJson) {
        if (bodyJson == null || bodyJson.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        JsonNode root = parseJsonObject(bodyJson, "extra-body");
        Map<String, JsonValue> body = new HashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> fields = root.fields(); fields.hasNext(); ) {
            Map.Entry<String, JsonNode> field = fields.next();
            body.put(field.getKey(), JsonValue.fromJsonNode(field.getValue()));
        }
        return body;
    }

    private static JsonNode parseJsonObject(String json, String option) {
        try {
            JsonNode root = new ObjectMapper().readTree(json);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException(
                        String.format("Option '%s' must be a JSON object.", option));
            }
            return root;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    String.format("Option '%s' contains invalid JSON.", option), e);
        }
    }

    private static boolean hasThrowable(
            Throwable exception, Class<? extends Throwable> targetClass) {
        for (Throwable throwable = exception; throwable != null; throwable = throwable.getCause()) {
            if (targetClass.isInstance(throwable)) {
                return true;
            }
        }
        return false;
    }
}
