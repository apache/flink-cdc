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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DwsSqlUtils}. */
class DwsSqlUtilsTest {

    @Test
    void testBuildStagingTableSql() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("ID", DataTypes.INT().notNull())
                        .physicalColumn("Name", DataTypes.VARCHAR(20))
                        .primaryKey("ID")
                        .build();

        assertThat(DwsSqlUtils.buildCreateStagingTableSql("ODS", "Stage", schema, false))
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS \"ods\".\"stage\" "
                                + "(\"__flink_cdc_op\" VARCHAR(16) NOT NULL, "
                                + "\"__flink_cdc_seq\" BIGINT NOT NULL, "
                                + "\"id\" INTEGER, \"name\" VARCHAR(20))");

        assertThat(
                        DwsSqlUtils.buildInsertStagingSql(
                                "ODS", "Stage", Arrays.asList("ID", "Name"), false))
                .isEqualTo(
                        "INSERT INTO \"ods\".\"stage\" "
                                + "(\"__flink_cdc_op\", \"__flink_cdc_seq\", \"id\", \"name\") "
                                + "VALUES (?, ?, ?, ?)");
    }

    @Test
    void testBuildStagingTableSqlWithDwsTypeMappings() {
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("boolean_col", DataTypes.BOOLEAN())
                        .physicalColumn("tinyint_col", DataTypes.TINYINT())
                        .physicalColumn("smallint_col", DataTypes.SMALLINT())
                        .physicalColumn("int_col", DataTypes.INT())
                        .physicalColumn("bigint_col", DataTypes.BIGINT())
                        .physicalColumn("float_col", DataTypes.FLOAT())
                        .physicalColumn("double_col", DataTypes.DOUBLE())
                        .physicalColumn("decimal_col", DataTypes.DECIMAL(10, 2))
                        .physicalColumn("char_col", DataTypes.CHAR(4))
                        .physicalColumn("varchar_col", DataTypes.VARCHAR(20))
                        .physicalColumn("string_col", DataTypes.STRING())
                        .physicalColumn("binary_col", DataTypes.BINARY(4))
                        .physicalColumn("varbinary_col", DataTypes.VARBINARY(20))
                        .physicalColumn("date_col", DataTypes.DATE())
                        .physicalColumn("time_col", DataTypes.TIME(9))
                        .physicalColumn("timestamp_col", DataTypes.TIMESTAMP(9))
                        .physicalColumn("timestamp_tz_col", DataTypes.TIMESTAMP_TZ(9))
                        .physicalColumn("timestamp_ltz_col", DataTypes.TIMESTAMP_LTZ(9))
                        .physicalColumn("array_col", DataTypes.ARRAY(DataTypes.STRING()))
                        .physicalColumn(
                                "map_col", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
                        .physicalColumn(
                                "row_col",
                                DataTypes.ROW(DataTypes.FIELD("nested", DataTypes.STRING())))
                        .build();

        assertThat(DwsSqlUtils.buildCreateStagingTableSql("ods", "stage", schema, false))
                .contains("\"boolean_col\" BOOLEAN")
                .contains("\"tinyint_col\" SMALLINT")
                .contains("\"smallint_col\" SMALLINT")
                .contains("\"int_col\" INTEGER")
                .contains("\"bigint_col\" BIGINT")
                .contains("\"float_col\" REAL")
                .contains("\"double_col\" DOUBLE PRECISION")
                .contains("\"decimal_col\" DECIMAL(10, 2)")
                .contains("\"char_col\" CHAR(4)")
                .contains("\"varchar_col\" VARCHAR(20)")
                .contains("\"string_col\" TEXT")
                .contains("\"binary_col\" BYTEA")
                .contains("\"varbinary_col\" BYTEA")
                .contains("\"date_col\" DATE")
                .contains("\"time_col\" TIME(6)")
                .contains("\"timestamp_col\" TIMESTAMP(6)")
                .contains("\"timestamp_tz_col\" TIMESTAMPTZ(6)")
                .contains("\"timestamp_ltz_col\" TIMESTAMPTZ(6)")
                .contains("\"array_col\" TEXT")
                .contains("\"map_col\" JSON")
                .contains("\"row_col\" JSON");
    }

    @Test
    void testBuildStagingTableSqlRejectsUnsupportedType() {
        Schema schema =
                Schema.newBuilder().physicalColumn("variant_col", DataTypes.VARIANT()).build();

        assertThatThrownBy(
                        () -> DwsSqlUtils.buildCreateStagingTableSql("ods", "stage", schema, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported data type for GaussDB DWS DDL");
    }

    @Test
    void testBuildLatestRowsCommitSql() {
        DwsCommittable committable =
                new DwsCommittable(
                        "job",
                        100L,
                        0,
                        "ods",
                        "orders",
                        "ods",
                        "flink_cdc_stage_abcd_100_0_0",
                        Arrays.asList("id", "name"),
                        Collections.singletonList("id"));

        assertThat(DwsSqlUtils.buildDeleteLatestRowsSql(committable, false))
                .contains("DELETE FROM \"ods\".\"orders\" AS t USING")
                .contains(
                        "ROW_NUMBER() OVER (PARTITION BY s.\"id\" "
                                + "ORDER BY s.\"__flink_cdc_seq\" DESC)")
                .contains("WHERE t.\"id\" = s.\"id\"");

        assertThat(DwsSqlUtils.buildInsertLatestRowsSql(committable, false))
                .contains("INSERT INTO \"ods\".\"orders\" (\"id\", \"name\")")
                .contains("SELECT s.\"id\", s.\"name\"")
                .contains("s.\"__flink_cdc_op\" <> 'DELETE'");
    }

    @Test
    void testBuildLatestRowsCommitSqlWithCompositePrimaryKeysAndCaseSensitiveNames() {
        DwsCommittable committable =
                new DwsCommittable(
                        "job",
                        100L,
                        0,
                        "ODS",
                        "Orders",
                        "ODS",
                        "Stage",
                        Arrays.asList("TenantId", "ID", "Name"),
                        Arrays.asList("TenantId", "ID"));

        assertThat(DwsSqlUtils.buildDeleteLatestRowsSql(committable, true))
                .contains("DELETE FROM \"ODS\".\"Orders\" AS t USING")
                .contains(
                        "ROW_NUMBER() OVER (PARTITION BY s.\"TenantId\", s.\"ID\" "
                                + "ORDER BY s.\"__flink_cdc_seq\" DESC)")
                .contains("WHERE t.\"TenantId\" = s.\"TenantId\" AND t.\"ID\" = s.\"ID\"");

        assertThat(DwsSqlUtils.buildInsertLatestRowsSql(committable, true))
                .contains("INSERT INTO \"ODS\".\"Orders\" (\"TenantId\", \"ID\", \"Name\")")
                .contains("SELECT s.\"TenantId\", s.\"ID\", s.\"Name\"")
                .contains("FROM \"ODS\".\"Stage\"");
    }

    @Test
    void testBuildCommitMarkerSqlIncludesStagingTableInIdempotencyKey() {
        assertThat(DwsSqlUtils.buildCreateCommitTableSql("Meta", false))
                .contains(
                        "PRIMARY KEY (\"job_id\", \"checkpoint_id\", \"subtask_id\", "
                                + "\"target_table\", \"staging_table\")");
        assertThat(DwsSqlUtils.buildSelectCommitMarkerSql("Meta", false))
                .isEqualTo(
                        "SELECT 1 FROM \"meta\".\"flink_cdc_commits\" "
                                + "WHERE \"job_id\" = ? AND \"checkpoint_id\" = ? "
                                + "AND \"subtask_id\" = ? AND \"target_table\" = ? "
                                + "AND \"staging_table\" = ? LIMIT 1");
    }

    @Test
    void testNormalizeIdentifiersAndDefaultSchema() {
        assertThat(DwsSqlUtils.normalizeSchemaName(TableId.tableId("Orders"), "ODS", false))
                .isEqualTo("ods");
        assertThat(DwsSqlUtils.normalizeTableName(TableId.tableId("Orders"), false))
                .isEqualTo("orders");
        assertThat(
                        DwsSqlUtils.normalizeSchemaName(
                                TableId.tableId("ODS", "Orders"), "Public", true))
                .isEqualTo("ODS");
        assertThat(DwsSqlUtils.formatTableIdentifier("Mi\"xed", "Orders", true))
                .isEqualTo("\"Mi\"\"xed\".\"Orders\"");
    }
}
