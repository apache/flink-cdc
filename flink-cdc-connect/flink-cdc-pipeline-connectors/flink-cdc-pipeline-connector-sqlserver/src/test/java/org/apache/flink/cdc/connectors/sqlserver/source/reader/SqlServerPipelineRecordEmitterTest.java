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

package org.apache.flink.cdc.connectors.sqlserver.source.reader;

import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerEventDeserializer;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlServerPipelineRecordEmitter}. */
class SqlServerPipelineRecordEmitterTest {

    private static final TableId DBZ_TABLE_ID = new TableId("db0", "dbo", "users");
    private static final org.apache.flink.cdc.common.event.TableId CDC_TABLE_ID =
            org.apache.flink.cdc.common.event.TableId.tableId("db0", "dbo", "users");

    @Test
    void testApplySplitUsesRestoredSchemaOverExistingCache() {
        SqlServerEventDeserializer deserializer =
                new SqlServerEventDeserializer(DebeziumChangelogMode.ALL, true);
        deserializer
                .getCreateTableEventCache()
                .put(
                        DBZ_TABLE_ID,
                        new CreateTableEvent(
                                CDC_TABLE_ID,
                                schema(Collections.singletonList(col("id", false, 1)))));

        SqlServerPipelineRecordEmitter<Event> emitter =
                new SqlServerPipelineRecordEmitter<>(
                        deserializer, null, createSourceConfig(), null, null);

        emitter.applySplit(
                new SnapshotSplit(
                        DBZ_TABLE_ID,
                        "db0.dbo.users:0",
                        RowType.of(new IntType()),
                        null,
                        null,
                        null,
                        Collections.singletonMap(
                                DBZ_TABLE_ID,
                                new TableChanges.TableChange(
                                        TableChanges.TableChangeType.CREATE,
                                        table(
                                                Arrays.asList(
                                                        col("id", false, 1),
                                                        col("age", true, 2)))))));

        assertThat(
                        deserializer
                                .getCreateTableEventCache()
                                .get(DBZ_TABLE_ID)
                                .getSchema()
                                .getColumnNames())
                .containsExactly("id", "age");
    }

    private static SqlServerSourceConfig createSourceConfig() {
        return new SqlServerSourceConfig(
                StartupOptions.initial(),
                Collections.singletonList("db0"),
                Collections.singletonList("dbo.users"),
                8096,
                100,
                1000.0,
                0.05,
                true,
                false,
                new Properties(),
                Configuration.empty(),
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "localhost",
                1433,
                "user",
                "password",
                1024,
                "UTC",
                Duration.ofSeconds(30),
                3,
                1,
                null,
                false,
                false,
                false);
    }

    private static Schema schema(java.util.List<Column> columns) {
        return SqlServerSchemaUtils.toSchema(table(columns));
    }

    private static Column col(String name, boolean optional, int position) {
        return Column.editor()
                .name(name)
                .jdbcType(java.sql.Types.INTEGER)
                .type("INT", "INT")
                .position(position)
                .optional(optional)
                .create();
    }

    private static Table table(java.util.List<Column> columns) {
        TableEditor editor = Table.editor().tableId(DBZ_TABLE_ID);
        columns.forEach(editor::addColumn);
        if (!columns.isEmpty()) {
            editor.setPrimaryKeyNames("id");
        }
        return editor.create();
    }
}
