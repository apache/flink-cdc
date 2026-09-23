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

import org.apache.flink.cdc.common.model.ModelContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** HTTP-level tests for {@link OpenAiCompatibleModelClient}. */
class OpenAiCompatibleModelClientTest {

    private static final String COMPLETION_RESPONSE =
            "{"
                    + "\"id\":\"chatcmpl-test\","
                    + "\"object\":\"chat.completion\","
                    + "\"created\":123,"
                    + "\"model\":\"test-model\","
                    + "\"choices\":[{"
                    + "\"index\":0,"
                    + "\"message\":{\"role\":\"assistant\","
                    + "\"content\":\"{\\\"result\\\":\\\"done\\\"}\"},"
                    + "\"finish_reason\":\"stop\"}],"
                    + "\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}"
                    + "}";

    private static final String EMBEDDING_RESPONSE =
            "{"
                    + "\"object\":\"list\","
                    + "\"data\":[{\"object\":\"embedding\",\"index\":0,"
                    + "\"embedding\":[0.1,-0.2,0.3]}],"
                    + "\"model\":\"test-model\","
                    + "\"usage\":{\"prompt_tokens\":3,\"total_tokens\":3}"
                    + "}";

    private static final String ERROR_RESPONSE =
            "{\"error\":{\"message\":\"request failed\","
                    + "\"type\":\"test_error\",\"param\":null,\"code\":\"test\"}}";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private MockWebServer server;
    private OpenAiCompatibleModelClient client;

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
        server.shutdown();
    }

    @Test
    void testTextCompletionWithCommonRequestParameters() throws Exception {
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("system-prompt", "configured prompt");
        options.put("user-prompt", "follow up");
        options.put("temperature", "0.7");
        options.put("top-p", "0.8");
        options.put("stop", "END");
        options.put("max-tokens", "256");
        options.put("presence-penalty", "1.2");
        options.put("frequency-penalty", "0.4");
        options.put("n", "2");
        options.put("seed", "123");
        options.put("response-format", "json_object");
        options.put("extra-header", "{\"X-Test\":\"header-value\"}");
        options.put("extra-body", "{\"vendor_flag\":true}");
        client = createAndOpenClient(options);

        assertThat(client.generate("runtime prompt", "input text"))
                .isEqualTo("{\"result\":\"done\"}");

        RecordedRequest request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/v1/chat/completions");
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer sk-test");
        assertThat(request.getHeader("X-Test")).isEqualTo("header-value");
        JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
        assertThat(body.path("model").asText()).isEqualTo("test-model");
        assertThat(body.at("/messages/0/role").asText()).isEqualTo("system");
        assertThat(body.at("/messages/0/content").asText())
                .isEqualTo("configured prompt\nruntime prompt");
        assertThat(body.at("/messages/1/content").asText()).isEqualTo("input text");
        assertThat(body.at("/messages/2/content").asText()).isEqualTo("follow up");
        assertThat(body.path("temperature").asDouble()).isEqualTo(0.7d);
        assertThat(body.path("top_p").asDouble()).isEqualTo(0.8d);
        assertThat(body.path("stop").asText()).isEqualTo("END");
        assertThat(body.path("max_tokens").asInt()).isEqualTo(256);
        assertThat(body.path("presence_penalty").asDouble()).isEqualTo(1.2d);
        assertThat(body.path("frequency_penalty").asDouble()).isEqualTo(0.4d);
        assertThat(body.path("n").asLong()).isEqualTo(2L);
        assertThat(body.path("seed").asLong()).isEqualTo(123L);
        assertThat(body.at("/response_format/type").asText()).isEqualTo("json_object");
        assertThat(body.path("vendor_flag").asBoolean()).isTrue();
    }

    @Test
    void testTextCompletionPreservesChinesePromptsAndInput() throws Exception {
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("system-prompt", "你是一个简洁的助手。");
        options.put("user-prompt", "请直接回答。");
        client = createAndOpenClient(options);

        assertThat(client.generate("总结输入内容", "包含中文的输入文本")).isEqualTo("{\"result\":\"done\"}");

        JsonNode body = objectMapper.readTree(server.takeRequest().getBody().readUtf8());
        assertThat(body.at("/messages/0/content").asText()).isEqualTo("你是一个简洁的助手。\n总结输入内容");
        assertThat(body.at("/messages/1/content").asText()).isEqualTo("包含中文的输入文本");
        assertThat(body.at("/messages/2/content").asText()).isEqualTo("请直接回答。");
    }

    @Test
    void testImageUrlCompletion() throws Exception {
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("content-type", "image_url");
        client = createAndOpenClient(options);

        assertThat(client.generate("describe the image", "https://example.com/image.png"))
                .contains("done");

        JsonNode body = objectMapper.readTree(server.takeRequest().getBody().readUtf8());
        assertThat(body.at("/messages/1/content/0/type").asText()).isEqualTo("image_url");
        assertThat(body.at("/messages/1/content/0/image_url/url").asText())
                .isEqualTo("https://example.com/image.png");
    }

    @Test
    void testImageCompletionUsesStandardVisionChatRequest() throws Exception {
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("system-prompt", "You are a vision assistant.");
        options.put("user-prompt", "Answer briefly.");
        options.put("temperature", "0.2");
        options.put("extra-header", "{\"X-Vision\":\"enabled\"}");
        options.put("extra-body", "{\"vendor_flag\":true}");
        client = createAndOpenClient(options);

        byte[] png =
                new byte[] {
                    (byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x01, 0x02, 0x03
                };
        assertThat(client.generateTextFromImage(png, "What is in this image?"))
                .isEqualTo("{\"result\":\"done\"}");

        RecordedRequest request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/v1/chat/completions");
        assertThat(request.getHeader("X-Vision")).isEqualTo("enabled");
        JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
        assertThat(body.at("/messages/0/role").asText()).isEqualTo("system");
        assertThat(body.at("/messages/0/content").asText())
                .isEqualTo("You are a vision assistant.");
        assertThat(body.at("/messages/1/content/0/type").asText()).isEqualTo("text");
        assertThat(body.at("/messages/1/content/0/text").asText())
                .isEqualTo("What is in this image?");
        assertThat(body.at("/messages/1/content/1/type").asText()).isEqualTo("image_url");
        assertThat(body.at("/messages/1/content/1/image_url/url").asText())
                .isEqualTo("data:image/png;base64,iVBORw0KGgoBAgM=");
        assertThat(body.at("/messages/2/content").asText()).isEqualTo("Answer briefly.");
        assertThat(body.path("temperature").asDouble()).isEqualTo(0.2d);
        assertThat(body.path("vendor_flag").asBoolean()).isTrue();
    }

    @Test
    void testImageMimeTypeDetection() {
        assertThat(
                        OpenAiCompatibleModelClient.detectImageMimeType(
                                new byte[] {(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}))
                .isEqualTo("image/png");
        assertThat(
                        OpenAiCompatibleModelClient.detectImageMimeType(
                                new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF}))
                .isEqualTo("image/jpeg");
        assertThat(
                        OpenAiCompatibleModelClient.detectImageMimeType(
                                new byte[] {0x47, 0x49, 0x46, 0x38, 0x39, 0x61}))
                .isEqualTo("image/gif");
        assertThat(
                        OpenAiCompatibleModelClient.detectImageMimeType(
                                new byte[] {
                                    0x52, 0x49, 0x46, 0x46, 0, 0, 0, 0, 0x57, 0x45, 0x42, 0x50
                                }))
                .isEqualTo("image/webp");
    }

    @Test
    void testNullAndInvalidImageInputsDoNotSendRequests() {
        client = createAndOpenClient(baseOptions());

        assertThat(client.generateTextFromImage(null, "describe")).isNull();
        assertThatThrownBy(() -> client.generateTextFromImage(new byte[0], "describe"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be null or empty");
        assertThatThrownBy(
                        () ->
                                client.generateTextFromImage(
                                        new byte[] {0x01, 0x02, 0x03}, "describe"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unrecognized image format");
        assertThat(server.getRequestCount()).isZero();
    }

    @Test
    void testRetryableImageCompletionErrorIsRetried() {
        server.enqueue(jsonResponse(429, ERROR_RESPONSE));
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("retry-num", "2");
        options.put("retry-backoff-base-interval", "1 ms");
        client = createAndOpenClient(options);

        byte[] jpeg = new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF};
        assertThat(client.generateTextFromImage(jpeg, "describe")).contains("done");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testTextEmbedding() throws Exception {
        server.enqueue(jsonResponse(200, EMBEDDING_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("dimension", "3");
        options.put("extra-header", "{\"X-Embedding\":\"enabled\"}");
        options.put("extra-body", "{\"vendor_flag\":true}");
        client = createAndOpenClient(options);

        assertThat(client.embed("embed this")).containsExactly(0.1f, -0.2f, 0.3f);

        RecordedRequest request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/v1/embeddings");
        assertThat(request.getHeader("X-Embedding")).isEqualTo("enabled");
        JsonNode body = objectMapper.readTree(request.getBody().readUtf8());
        assertThat(body.path("model").asText()).isEqualTo("test-model");
        assertThat(body.path("input").asText()).isEqualTo("embed this");
        assertThat(body.path("dimensions").asInt()).isEqualTo(3);
        assertThat(body.path("vendor_flag").asBoolean()).isTrue();
    }

    @Test
    void testRetryableHttpErrorIsRetried() {
        server.enqueue(jsonResponse(429, ERROR_RESPONSE));
        server.enqueue(jsonResponse(200, COMPLETION_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("retry-num", "2");
        options.put("retry-backoff-base-interval", "1 ms");
        client = createAndOpenClient(options);

        assertThat(client.generate("system", "input")).contains("done");
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testNonRetryableHttpErrorFailsImmediately() {
        server.enqueue(jsonResponse(400, ERROR_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("retry-num", "3");
        options.put("retry-backoff-base-interval", "1 ms");
        client = createAndOpenClient(options);

        assertThatThrownBy(() -> client.generate("system", "input"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("failed after 1 attempt");
        assertThat(server.getRequestCount()).isEqualTo(1);
    }

    @Test
    void testIgnoreFallbackReturnsNullAfterRetries() {
        server.enqueue(jsonResponse(500, ERROR_RESPONSE));
        server.enqueue(jsonResponse(500, ERROR_RESPONSE));
        Map<String, String> options = baseOptions();
        options.put("retry-num", "2");
        options.put("retry-backoff-base-interval", "1 ms");
        options.put("retry-fallback-strategy", "ignore");
        client = createAndOpenClient(options);

        assertThat(client.generate("system", "input")).isNull();
        assertThat(server.getRequestCount()).isEqualTo(2);
    }

    @Test
    void testInvalidAdditionalHeadersFailOnOpen() {
        Map<String, String> options = baseOptions();
        options.put("extra-header", "not-json");
        client = createClient(options);

        assertThatThrownBy(client::open)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("extra-header")
                .hasMessageContaining("invalid JSON");
    }

    @Test
    void testRequestBeforeOpenFailsWithContext() {
        client = createClient(baseOptions());

        assertThatThrownBy(() -> client.embed("input"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("embedding request failed")
                .hasRootCauseMessage("OpenAI-compatible model client has not been opened.");
    }

    private OpenAiCompatibleModelClient createAndOpenClient(Map<String, String> options) {
        OpenAiCompatibleModelClient result = createClient(options);
        result.open();
        return result;
    }

    private OpenAiCompatibleModelClient createClient(Map<String, String> options) {
        return (OpenAiCompatibleModelClient)
                new OpenAiCompatibleModelClientFactory().createClient(context(options));
    }

    private Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("endpoint", server.url("/v1").toString());
        options.put("api-key", "sk-test");
        options.put("model", "test-model");
        return options;
    }

    private static ModelContext context(Map<String, String> options) {
        return new ModelContext() {
            @Override
            public String getModelName() {
                return "test-model-definition";
            }

            @Override
            public Map<String, String> getOptions() {
                return options;
            }

            @Override
            public ClassLoader getClassLoader() {
                return Thread.currentThread().getContextClassLoader();
            }
        };
    }

    private static MockResponse jsonResponse(int statusCode, String body) {
        return new MockResponse()
                .setResponseCode(statusCode)
                .addHeader("Content-Type", "application/json")
                .setBody(body);
    }
}
