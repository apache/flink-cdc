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

        // Build SELECT clause with named parameters for USING subquery
        String selectClause = Arrays.stream(fieldNames)
                .map(f -> ":" + f + " AS " + quoteIdentifier(f))
                .collect(Collectors.joining(", "));

        // Build ON clause for matching
        String onClause = Arrays.stream(uniqueKeyFields)
                .map(f -> String.format("t.%s = s.%s", quoteIdentifier(f), quoteIdentifier(f)))
                .collect(Collectors.joining(" AND "));

        // Build UPDATE SET clause (exclude primary key fields)
        String updateSetClause = Arrays.stream(fieldNames)
                .filter(f -> !Arrays.asList(uniqueKeyFields).contains(f))
                .map(f -> String.format("%s = s.%s", quoteIdentifier(f), quoteIdentifier(f)))
                .collect(Collectors.joining(", "));

        // Build INSERT column list
        String insertColumns = Arrays.stream(fieldNames)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        // Build INSERT VALUES list
        String insertValues = Arrays.stream(fieldNames)
                .map(f -> "s." + quoteIdentifier(f))
                .collect(Collectors.joining(", "));

        // Handle case where all columns are primary keys (no columns to update)
        String mergeStatement;
        if (updateSetClause.isEmpty()) {
            // Only INSERT when not matched, skip UPDATE
            mergeStatement = String.format(
                    "MERGE INTO %s t USING (SELECT %s) AS s ON %s " +
                            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                    quoteIdentifier(tableName),
                    selectClause,
                    onClause,
                    insertColumns,
                    insertValues);
        } else {
            mergeStatement = String.format(
                    "MERGE INTO %s t USING (SELECT %s) AS s ON %s " +
                            "WHEN MATCHED THEN UPDATE SET %s " +
                            "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
                    quoteIdentifier(tableName),
                    selectClause,
                    onClause,
                    updateSetClause,
                    insertColumns,
                    insertValues);
        }

        return Optional.of(mergeStatement);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}
