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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DwsMetadataApplier}. */
class DwsMetadataApplierTest {

    @Test
    void testBuildCreateTableSql() {
        DwsMetadataApplier metadataApplier =
                new DwsMetadataApplier(
                        "jdbc:gaussdb://localhost:8000/test",
                        "user",
                        "password",
                        false,
                        "ods",
                        true,
                        "id, tenant_id");

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("tenant_id", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        String sql =
                metadataApplier.buildCreateTableSql(
                        new CreateTableEvent(TableId.tableId("orders"), schema));

        assertThat(sql)
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS ods.orders "
                                + "(id INTEGER NOT NULL, tenant_id INTEGER, PRIMARY KEY (id)) "
                                + "DISTRIBUTE BY HASH (id, tenant_id)");
    }

    @Test
    void testBuildCreateTableSqlWithCaseSensitiveIdentifiersAndDefaults() {
        DwsMetadataApplier metadataApplier =
                new DwsMetadataApplier(
                        "jdbc:gaussdb://localhost:8000/test",
                        "user",
                        "password",
                        true,
                        "public",
                        false,
                        null);

        Schema schema =
                Schema.newBuilder()
                        .column(
                                Column.physicalColumn(
                                        "User\"Name",
                                        DataTypes.VARCHAR(10).notNull(),
                                        null,
                                        "O'Reilly"))
                        .physicalColumn("CreatedAt", DataTypes.TIMESTAMP_LTZ(9))
                        .primaryKey("User\"Name")
                        .build();

        String sql =
                metadataApplier.buildCreateTableSql(
                        new CreateTableEvent(
                                TableId.tableId("catalog", "Sales", "Orders"), schema));

        assertThat(sql)
                .isEqualTo(
                        "CREATE TABLE IF NOT EXISTS \"Sales\".\"Orders\" "
                                + "(\"User\"\"Name\" VARCHAR(10) DEFAULT 'O''Reilly' NOT NULL, "
                                + "\"CreatedAt\" TIMESTAMPTZ(6), PRIMARY KEY (\"User\"\"Name\"))");
    }

    @Test
    void testBuildAlterTableSql() {
        DwsMetadataApplier metadataApplier =
                new DwsMetadataApplier(
                        "jdbc:gaussdb://localhost:8000/test",
                        "user",
                        "password",
                        false,
                        "ods",
                        false,
                        null);
        TableId tableId = TableId.tableId("orders");

        assertThat(
                        metadataApplier.buildAddColumnSql(
                                tableId,
                                Column.physicalColumn(
                                        "score", DataTypes.DECIMAL(10, 2).notNull(), null, "0")))
                .isEqualTo(
                        "ALTER TABLE ods.orders ADD COLUMN score DECIMAL(10, 2) DEFAULT 0 NOT NULL");
        assertThat(metadataApplier.buildDropColumnSql(tableId, "score"))
                .isEqualTo("ALTER TABLE ods.orders DROP COLUMN IF EXISTS score");
        assertThat(metadataApplier.buildRenameColumnSql(tableId, "score", "total_score"))
                .isEqualTo("ALTER TABLE ods.orders RENAME COLUMN score TO total_score");
        assertThat(metadataApplier.buildAlterColumnTypeSql(tableId, "score", DataTypes.VARCHAR(32)))
                .isEqualTo("ALTER TABLE ods.orders ALTER COLUMN score TYPE VARCHAR(32)");
        assertThat(metadataApplier.buildTruncateTableSql(tableId))
                .isEqualTo("TRUNCATE TABLE ods.orders");
        assertThat(metadataApplier.buildDropTableSql(tableId))
                .isEqualTo("DROP TABLE IF EXISTS ods.orders");
    }

    @Test
    void testAcceptsConfiguredSchemaEvolutionTypes() {
        DwsMetadataApplier metadataApplier =
                new DwsMetadataApplier(
                        "jdbc:gaussdb://localhost:8000/test",
                        "user",
                        "password",
                        false,
                        "public",
                        false,
                        null);

        metadataApplier.setAcceptedSchemaEvolutionTypes(
                EnumSet.of(SchemaChangeEventType.CREATE_TABLE, SchemaChangeEventType.DROP_TABLE));

        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.CREATE_TABLE))
                .isTrue();
        assertThat(metadataApplier.acceptsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN))
                .isFalse();
    }
}
