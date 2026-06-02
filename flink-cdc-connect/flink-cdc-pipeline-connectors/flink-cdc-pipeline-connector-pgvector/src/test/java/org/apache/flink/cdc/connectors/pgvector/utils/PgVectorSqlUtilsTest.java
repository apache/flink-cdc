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

package org.apache.flink.cdc.connectors.pgvector.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/** Tests for {@link PgVectorSqlUtils}. */
class PgVectorSqlUtilsTest {

    @Test
    void testCreateTableSqlWithVectorColumn() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("content", DataTypes.STRING())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();
        PgVectorTableInfo tableInfo =
                PgVectorSqlUtils.resolveTableInfo(TableId.parse("public.docs"), "public");
        Map<String, PgVectorColumnSpec> vectorColumns =
                ImmutableMap.of(
                        "public.docs.embedding", new PgVectorColumnSpec(PgVectorType.VECTOR, 3));

        Assertions.assertThat(
                        PgVectorSqlUtils.createTableSql(
                                tableInfo, schema, vectorColumns, ImmutableMap.of()))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS \"public\".\"docs\" "
                                + "(\"id\" INTEGER NOT NULL, \"content\" TEXT, "
                                + "\"embedding\" vector(3), PRIMARY KEY (\"id\"))");
    }

    @Test
    void testUpsertAndDeleteSql() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("id")
                        .build();
        PgVectorTableInfo tableInfo = new PgVectorTableInfo("public", "docs");

        Assertions.assertThat(PgVectorSqlUtils.upsertSql(tableInfo, schema))
                .isEqualTo(
                        "INSERT INTO \"public\".\"docs\" (\"id\", \"embedding\") VALUES (?, ?) "
                                + "ON CONFLICT (\"id\") DO UPDATE SET "
                                + "\"embedding\" = EXCLUDED.\"embedding\"");
        Assertions.assertThat(PgVectorSqlUtils.deleteSql(tableInfo, schema))
                .isEqualTo("DELETE FROM \"public\".\"docs\" WHERE \"id\" = ?");
    }

    @Test
    void testUpsertWithCompositePrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("tenant_id", DataTypes.INT().notNull())
                        .physicalColumn("doc_id", DataTypes.INT().notNull())
                        .physicalColumn("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
                        .primaryKey("tenant_id", "doc_id")
                        .build();
        PgVectorTableInfo tableInfo = new PgVectorTableInfo("public", "docs");

        Assertions.assertThat(PgVectorSqlUtils.upsertSql(tableInfo, schema))
                .isEqualTo(
                        "INSERT INTO \"public\".\"docs\" (\"tenant_id\", \"doc_id\", \"embedding\") "
                                + "VALUES (?, ?, ?) ON CONFLICT (\"tenant_id\", \"doc_id\") "
                                + "DO UPDATE SET \"embedding\" = EXCLUDED.\"embedding\"");
        Assertions.assertThat(PgVectorSqlUtils.deleteSql(tableInfo, schema))
                .isEqualTo(
                        "DELETE FROM \"public\".\"docs\" WHERE \"tenant_id\" = ? AND \"doc_id\" = ?");
    }

    @Test
    void testUpsertWithPrimaryKeyOnlyTableDoesNothingOnConflict() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        PgVectorTableInfo tableInfo = new PgVectorTableInfo("public", "docs");

        Assertions.assertThat(PgVectorSqlUtils.upsertSql(tableInfo, schema))
                .isEqualTo(
                        "INSERT INTO \"public\".\"docs\" (\"id\") VALUES (?) "
                                + "ON CONFLICT (\"id\") DO NOTHING");
    }

    @Test
    void testCreateTableSqlWithTableProperties() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        PgVectorTableInfo tableInfo = new PgVectorTableInfo("public", "docs");

        Assertions.assertThat(
                        PgVectorSqlUtils.createTableSql(
                                tableInfo,
                                schema,
                                ImmutableMap.of(),
                                ImmutableMap.of("fillfactor", "90")))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS \"public\".\"docs\" "
                                + "(\"id\" INTEGER NOT NULL, PRIMARY KEY (\"id\")) "
                                + "WITH (fillfactor = 90)");
    }

    @Test
    void testRejectInvalidTablePropertyKey() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        PgVectorTableInfo tableInfo = new PgVectorTableInfo("public", "docs");

        Assertions.assertThatThrownBy(
                        () ->
                                PgVectorSqlUtils.createTableSql(
                                        tableInfo,
                                        schema,
                                        ImmutableMap.of(),
                                        ImmutableMap.of("fillfactor); DROP TABLE docs; --", "90")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid PostgreSQL storage parameter");
    }

    @Test
    void testJdbcArrayTypeName() {
        Assertions.assertThat(PgVectorTypeUtils.toJdbcArrayTypeName(DataTypes.FLOAT()))
                .isEqualTo("float4");
        Assertions.assertThat(PgVectorTypeUtils.toJdbcArrayTypeName(DataTypes.INT()))
                .isEqualTo("int4");
        Assertions.assertThat(PgVectorTypeUtils.toJdbcArrayTypeName(DataTypes.STRING()))
                .isEqualTo("text");
    }
}
