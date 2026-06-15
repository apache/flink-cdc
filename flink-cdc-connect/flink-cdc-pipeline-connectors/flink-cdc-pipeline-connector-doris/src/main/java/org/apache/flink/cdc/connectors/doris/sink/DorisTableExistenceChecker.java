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

import org.apache.commons.codec.binary.Base64;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;

/** Checks Doris database/table existence before schema reconciliation. */
@FunctionalInterface
interface DorisTableExistenceChecker extends Serializable {

    DorisTableExistenceChecker HTTP = new HttpDorisTableExistenceChecker();

    Existence check(DorisOptions dorisOptions, TableId tableId);

    enum Existence {
        DATABASE_ABSENT,
        TABLE_ABSENT,
        TABLE_EXISTS
    }

    class HttpDorisTableExistenceChecker implements DorisTableExistenceChecker {
        private static final Logger LOG =
                LoggerFactory.getLogger(HttpDorisTableExistenceChecker.class);
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final String STATEMENT_EXEC_API =
                "http://%s/api/query/default_cluster/information_schema";
        private static final String DATABASE_EXISTS_QUERY =
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA` "
                        + "WHERE SCHEMA_NAME = ? LIMIT 1";
        private static final String TABLE_EXISTS_QUERY =
                "SELECT TABLE_NAME FROM information_schema.`TABLES` "
                        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1";

        @Override
        public Existence check(DorisOptions dorisOptions, TableId tableId) {
            try {
                if (tableId.getSchemaName() == null || tableId.getSchemaName().isEmpty()) {
                    throw new IllegalArgumentException(
                            "Doris table id must include a database name: " + tableId.identifier());
                }
                String endpoint = RestService.randomEndpoint(dorisOptions.getFenodes(), LOG);
                if (!exists(
                        endpoint, dorisOptions, DATABASE_EXISTS_QUERY, tableId.getSchemaName())) {
                    return Existence.DATABASE_ABSENT;
                }
                if (!exists(
                        endpoint,
                        dorisOptions,
                        TABLE_EXISTS_QUERY,
                        tableId.getSchemaName(),
                        tableId.getTableName())) {
                    return Existence.TABLE_ABSENT;
                }
                return Existence.TABLE_EXISTS;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to check Doris table existence for " + tableId.identifier(), e);
            }
        }

        private boolean exists(
                String endpoint, DorisOptions dorisOptions, String sql, String... params)
                throws Exception {
            String requestUrl = String.format(STATEMENT_EXEC_API, endpoint);
            HttpPost httpPost = new HttpPost(requestUrl);
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader(dorisOptions));
            httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json;charset=UTF-8");
            httpPost.setEntity(
                    new StringEntity(
                            OBJECT_MAPPER.writeValueAsString(
                                    java.util.Collections.singletonMap(
                                            "stmt", bindSqlParams(sql, params))),
                            StandardCharsets.UTF_8));

            JsonNode response = RestService.handleResponse(httpPost, LOG);
            String code = response.path("code").asText("-1");
            if (!"0".equals(code)) {
                throw new IllegalStateException(
                        "Doris table existence query failed, response=" + response);
            }
            JsonNode rows = response.path("data").path("data");
            if (!rows.isArray()) {
                throw new IllegalStateException(
                        "Doris table existence query returned unexpected response, response="
                                + response);
            }
            return !rows.isEmpty();
        }

        private String bindSqlParams(String sql, String... params) {
            String boundSql = sql;
            for (String param : params) {
                boundSql =
                        boundSql.replaceFirst(
                                "\\?",
                                Matcher.quoteReplacement("'" + escapeSqlString(param) + "'"));
            }
            return boundSql;
        }

        private String escapeSqlString(String value) {
            return value.replace("\\", "\\\\").replace("'", "''");
        }

        private String authHeader(DorisOptions dorisOptions) {
            return "Basic "
                    + new String(
                            Base64.encodeBase64(
                                    (dorisOptions.getUsername() + ":" + dorisOptions.getPassword())
                                            .getBytes(StandardCharsets.UTF_8)),
                            StandardCharsets.UTF_8);
        }
    }
}
