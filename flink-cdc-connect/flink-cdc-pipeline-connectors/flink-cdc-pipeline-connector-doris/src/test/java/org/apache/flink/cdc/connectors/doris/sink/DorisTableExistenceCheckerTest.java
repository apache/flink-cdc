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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.event.TableId;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Unit tests for {@link DorisTableExistenceChecker}. */
public class DorisTableExistenceCheckerTest {

    private static final TableId TABLE_ID = TableId.parse("streampark.t_flink_app");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testHttpCheckerReturnsTableExistsAndUsesDorisQueryApi() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer(
                        responseWithRows("[[\"streampark\"]]"),
                        responseWithRows("[[\"t_flink_app\"]]"))) {
            DorisTableExistenceChecker.Existence existence =
                    DorisTableExistenceChecker.HTTP.check(server.createDorisOptions(), TABLE_ID);

            Assertions.assertThat(existence)
                    .isEqualTo(DorisTableExistenceChecker.Existence.TABLE_EXISTS);
            Assertions.assertThat(server.requests).hasSize(2);
            Assertions.assertThat(server.requests.get(0).path)
                    .isEqualTo("/api/query/default_cluster/information_schema");
            Assertions.assertThat(server.requests.get(0).authorization).isEqualTo("Basic cm9vdDo=");
            Assertions.assertThat(server.requests.get(0).contentType)
                    .isEqualTo("application/json;charset=UTF-8");
            Assertions.assertThat(server.requests.get(0).stmt)
                    .isEqualTo(
                            "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` "
                                    + "WHERE SCHEMA_NAME = 'streampark' LIMIT 1");
            Assertions.assertThat(server.requests.get(1).stmt)
                    .isEqualTo(
                            "SELECT TABLE_NAME FROM information_schema.`TABLES` "
                                    + "WHERE TABLE_SCHEMA = 'streampark' "
                                    + "AND TABLE_NAME = 't_flink_app' LIMIT 1");
        }
    }

    @Test
    public void testHttpCheckerReturnsDatabaseAbsentWhenDatabaseQueryHasNoRows() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer(responseWithRows("[]"))) {
            DorisTableExistenceChecker.Existence existence =
                    DorisTableExistenceChecker.HTTP.check(server.createDorisOptions(), TABLE_ID);

            Assertions.assertThat(existence)
                    .isEqualTo(DorisTableExistenceChecker.Existence.DATABASE_ABSENT);
            Assertions.assertThat(server.requests).hasSize(1);
        }
    }

    @Test
    public void testHttpCheckerReturnsTableAbsentWhenTableQueryHasNoRows() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer(
                        responseWithRows("[[\"streampark\"]]"), responseWithRows("[]"))) {
            DorisTableExistenceChecker.Existence existence =
                    DorisTableExistenceChecker.HTTP.check(server.createDorisOptions(), TABLE_ID);

            Assertions.assertThat(existence)
                    .isEqualTo(DorisTableExistenceChecker.Existence.TABLE_ABSENT);
            Assertions.assertThat(server.requests).hasSize(2);
        }
    }

    @Test
    public void testHttpCheckerFailsClosedWhenQueryApiReturnsNonOkCode() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer("{\"code\":\"1\",\"msg\":\"permission denied\"}")) {
            Assertions.assertThatThrownBy(
                            () ->
                                    DorisTableExistenceChecker.HTTP.check(
                                            server.createDorisOptions(), TABLE_ID))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to check Doris table existence")
                    .hasRootCauseMessage(
                            "Doris table existence query failed, response={\"code\":\"1\",\"msg\":\"permission denied\"}");
        }
    }

    @Test
    public void testHttpCheckerFailsClosedWhenQueryApiResponseShapeIsUnexpected() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer("{\"code\":\"0\",\"data\":{}}")) {
            Assertions.assertThatThrownBy(
                            () ->
                                    DorisTableExistenceChecker.HTTP.check(
                                            server.createDorisOptions(), TABLE_ID))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Failed to check Doris table existence")
                    .hasRootCauseMessage(
                            "Doris table existence query returned unexpected response, "
                                    + "response={\"code\":\"0\",\"data\":{}}");
        }
    }

    @Test
    public void testHttpCheckerEscapesSqlStringParams() throws Exception {
        try (RecordingDorisHttpServer server =
                new RecordingDorisHttpServer(
                        responseWithRows("[[\"db'\\\\prod\"]]"), responseWithRows("[]"))) {
            DorisTableExistenceChecker.HTTP.check(
                    server.createDorisOptions(), TableId.parse("db'\\prod.tbl'1"));

            Assertions.assertThat(server.requests.get(0).stmt)
                    .contains("SCHEMA_NAME = 'db''\\\\prod'");
            Assertions.assertThat(server.requests.get(1).stmt)
                    .contains("TABLE_SCHEMA = 'db''\\\\prod'")
                    .contains("TABLE_NAME = 'tbl''1'");
        }
    }

    @Test
    public void testHttpCheckerRejectsTableIdWithoutDatabase() {
        Assertions.assertThatThrownBy(
                        () ->
                                DorisTableExistenceChecker.HTTP.check(
                                        createDorisOptions("127.0.0.1:1"),
                                        TableId.parse("t_flink_app")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to check Doris table existence")
                .hasRootCauseMessage("Doris table id must include a database name: t_flink_app");
    }

    private static DorisOptions createDorisOptions(String fenodes) {
        return DorisOptions.builder()
                .setFenodes(fenodes)
                .setUsername("root")
                .setPassword("")
                .build();
    }

    private static String responseWithRows(String rows) {
        return "{\"code\":\"0\",\"data\":{\"data\":" + rows + "}}";
    }

    private static class RecordingDorisHttpServer implements AutoCloseable {
        private final HttpServer server;
        private final List<String> responses;
        private final List<RecordedRequest> requests = new ArrayList<>();

        private RecordingDorisHttpServer(String... responses) throws IOException {
            this.responses = new ArrayList<>();
            Collections.addAll(this.responses, responses);
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/query/default_cluster/information_schema", this::handle);
            server.start();
        }

        private DorisOptions createDorisOptions() {
            return DorisTableExistenceCheckerTest.createDorisOptions(
                    "127.0.0.1:" + server.getAddress().getPort());
        }

        private void handle(HttpExchange exchange) throws IOException {
            String body =
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode request = OBJECT_MAPPER.readTree(body);
            requests.add(
                    new RecordedRequest(
                            exchange.getRequestURI().getPath(),
                            exchange.getRequestHeaders().getFirst("Authorization"),
                            exchange.getRequestHeaders().getFirst("Content-Type"),
                            request.path("stmt").asText()));

            int responseIndex = Math.min(requests.size() - 1, responses.size() - 1);
            byte[] response = responses.get(responseIndex).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    private static class RecordedRequest {
        private final String path;
        private final String authorization;
        private final String contentType;
        private final String stmt;

        private RecordedRequest(
                String path, String authorization, String contentType, String stmt) {
            this.path = path;
            this.authorization = authorization;
            this.contentType = contentType;
            this.stmt = stmt;
        }
    }
}
