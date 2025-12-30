/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.gaussdb.jdbc;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.databases.postgres.dialect.PostgresDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * GaussDB JDBC Dialect.
 *
 * <p>
 * GaussDB is based on PostgreSQL but uses MERGE INTO syntax for upsert instead
 * of ON CONFLICT.
 * This dialect extends PostgresDialect and overrides the upsert statement
 * generation.
 */
public class GaussDBJdbcDialect extends PostgresDialect {

        private static final long serialVersionUID = 1L;

        @Override
        public String dialectName() {
                return "GaussDB";
        }

        /**
         * GaussDB uses MERGE INTO syntax for upsert operations.
         *
         * <p>
         * Flink JDBC Connector 3.x uses named parameters (:fieldName) instead of
         * positional
         * placeholders (?).
         *
         * <p>
         * Example:
         * 
         * <pre>
         * MERGE INTO target_table t
         * USING (SELECT :col1 AS "col1", :col2 AS "col2") AS s
         * ON t.pk = s.pk
         * WHEN MATCHED THEN UPDATE SET col2 = s.col2
         * WHEN NOT MATCHED THEN INSERT (col1, col2) VALUES (s.col1, s.col2);
         * </pre>
         */
        @Override
        public Optional<String> getUpsertStatement(
                        String tableName, String[] fieldNames, String[] uniqueKeyFields) {
                // Optimization hack: if table name ends with _FAST_INSERT_ONLY, use blind
                // INSERT
                if (tableName.endsWith("_FAST_INSERT_ONLY")) {
                        String realTableName = tableName.substring(0,
                                        tableName.length() - "_FAST_INSERT_ONLY".length());
                        return Optional.of(getInsertIntoStatement(realTableName, fieldNames));
                }

                // GaussDB in MYSQL compatibility mode supports 'ON DUPLICATE KEY UPDATE'
                // This is prioritized for performance as verified by EXPLAIN.
                return getOnDuplicateKeyUpdateStatement(tableName, fieldNames, uniqueKeyFields);
        }

        private Optional<String> getOnDuplicateKeyUpdateStatement(
                        String tableName, String[] fieldNames, String[] uniqueKeyFields) {
                final java.util.Set<String> uniqueKeyFieldsSet = new java.util.HashSet<>(
                                Arrays.asList(uniqueKeyFields));
                String updateClause = Arrays.stream(fieldNames)
                                .filter(f -> !uniqueKeyFieldsSet.contains(f))
                                .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                                .collect(Collectors.joining(", "));

                if (updateClause.isEmpty()) {
                        // All fields are unique keys, so DO NOTHING on conflict.
                        String firstKey = quoteIdentifier(uniqueKeyFields[0]);
                        updateClause = firstKey + "=" + firstKey;
                }

                return Optional.of(
                                getInsertIntoStatement(tableName, fieldNames)
                                                + " ON DUPLICATE KEY UPDATE "
                                                + updateClause);
        }

        @Override
        public String quoteIdentifier(String identifier) {
                return "\"" + identifier + "\"";
        }
}
