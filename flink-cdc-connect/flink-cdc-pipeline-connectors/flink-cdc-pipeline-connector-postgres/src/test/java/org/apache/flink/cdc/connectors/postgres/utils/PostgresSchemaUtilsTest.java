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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.event.TableId;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSchemaUtils}. */
class PostgresSchemaUtilsTest {

    // --------------------------------------------------------------------------------------------
    // Tests for quote
    // --------------------------------------------------------------------------------------------

    @Test
    void testQuote() {
        assertThat(PostgresSchemaUtils.quote("my_table")).isEqualTo("\"my_table\"");
    }

    @Test
    void testQuoteWithSpecialCharacters() {
        assertThat(PostgresSchemaUtils.quote("my-table.name")).isEqualTo("\"my-table.name\"");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for toDbzTableId
    // --------------------------------------------------------------------------------------------

    @Test
    void testToDbzTableIdWithNamespaceAndSchema() {
        TableId cdcTableId = TableId.tableId("my_namespace", "my_schema", "my_table");
        io.debezium.relational.TableId dbzTableId = PostgresSchemaUtils.toDbzTableId(cdcTableId);

        assertThat(dbzTableId.catalog()).isEqualTo("my_namespace");
        assertThat(dbzTableId.schema()).isEqualTo("my_schema");
        assertThat(dbzTableId.table()).isEqualTo("my_table");
    }

    @Test
    void testToDbzTableIdWithSchemaOnly() {
        TableId cdcTableId = TableId.tableId("my_schema", "my_table");
        io.debezium.relational.TableId dbzTableId = PostgresSchemaUtils.toDbzTableId(cdcTableId);

        assertThat(dbzTableId.schema()).isEqualTo("my_schema");
        assertThat(dbzTableId.table()).isEqualTo("my_table");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for toCdcTableId
    // --------------------------------------------------------------------------------------------

    @Test
    void testToCdcTableIdSimple() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId(null, "public", "users");
        TableId cdcTableId = PostgresSchemaUtils.toCdcTableId(dbzTableId);

        assertThat(cdcTableId.getSchemaName()).isEqualTo("public");
        assertThat(cdcTableId.getTableName()).isEqualTo("users");
    }

    @Test
    void testToCdcTableIdWithNullSchema() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId(null, null, "users");
        TableId cdcTableId = PostgresSchemaUtils.toCdcTableId(dbzTableId);

        assertThat(cdcTableId.getTableName()).isEqualTo("users");
    }

    @Test
    void testToCdcTableIdWithDatabaseIncluded() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId(null, "public", "users");
        TableId cdcTableId = PostgresSchemaUtils.toCdcTableId(dbzTableId, "my_db", true);

        assertThat(cdcTableId.getNamespace()).isEqualTo("my_db");
        assertThat(cdcTableId.getSchemaName()).isEqualTo("public");
        assertThat(cdcTableId.getTableName()).isEqualTo("users");
    }

    @Test
    void testToCdcTableIdWithDatabaseNotIncluded() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId(null, "public", "users");
        TableId cdcTableId = PostgresSchemaUtils.toCdcTableId(dbzTableId, "my_db", false);

        // Database not included, should use schema and table only
        assertThat(cdcTableId.getSchemaName()).isEqualTo("public");
        assertThat(cdcTableId.getTableName()).isEqualTo("users");
    }

    @Test
    void testToCdcTableIdWithNullDatabaseName() {
        io.debezium.relational.TableId dbzTableId =
                new io.debezium.relational.TableId(null, "public", "users");
        TableId cdcTableId = PostgresSchemaUtils.toCdcTableId(dbzTableId, null, true);

        // Null database name, should fall back to schema + table
        assertThat(cdcTableId.getSchemaName()).isEqualTo("public");
        assertThat(cdcTableId.getTableName()).isEqualTo("users");
    }

    // --------------------------------------------------------------------------------------------
    // Tests for round-trip conversion
    // --------------------------------------------------------------------------------------------

    @Test
    void testRoundTripConversion() {
        TableId original = TableId.tableId("my_namespace", "my_schema", "my_table");
        io.debezium.relational.TableId dbzId = PostgresSchemaUtils.toDbzTableId(original);
        TableId roundTripped = PostgresSchemaUtils.toCdcTableId(dbzId, "my_namespace", true);

        assertThat(roundTripped).isEqualTo(original);
    }
}
