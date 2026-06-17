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

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

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
}
