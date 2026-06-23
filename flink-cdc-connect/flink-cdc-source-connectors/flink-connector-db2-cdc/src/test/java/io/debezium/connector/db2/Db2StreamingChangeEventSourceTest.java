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

package io.debezium.connector.db2;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for DB2 streaming schema-change decisions. */
class Db2StreamingChangeEventSourceTest {

    @Test
    void testSchemaChangeDetectionIgnoresUnchangedSchema() {
        Table table =
                Table.editor()
                        .tableId(new TableId("TESTDB", "DB2INST1", "PRODUCTS"))
                        .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).create())
                        .addColumn(Column.editor().name("NAME").jdbcType(Types.VARCHAR).create())
                        .setPrimaryKeyNames("ID")
                        .create();

        assertThat(Db2StreamingChangeEventSource.isTableSchemaChanged(table, table)).isFalse();
    }

    @Test
    void testSchemaChangeDetectionFindsAddedColumn() {
        Table before =
                Table.editor()
                        .tableId(new TableId("TESTDB", "DB2INST1", "PRODUCTS"))
                        .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).create())
                        .addColumn(Column.editor().name("NAME").jdbcType(Types.VARCHAR).create())
                        .setPrimaryKeyNames("ID")
                        .create();
        Table after =
                Table.editor()
                        .tableId(new TableId("TESTDB", "DB2INST1", "PRODUCTS"))
                        .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).create())
                        .addColumn(Column.editor().name("NAME").jdbcType(Types.VARCHAR).create())
                        .addColumn(Column.editor().name("VOLUME").jdbcType(Types.FLOAT).create())
                        .setPrimaryKeyNames("ID")
                        .create();

        assertThat(Db2StreamingChangeEventSource.isTableSchemaChanged(before, after)).isTrue();
    }

    @Test
    void testZeroStopLsnIsOpenEnded() {
        assertThat(
                        Db2StreamingChangeEventSource.isValidStopLsn(
                                Lsn.valueOf("00000000000000000000000000000000")))
                .isFalse();
        assertThat(
                        Db2StreamingChangeEventSource.isValidStopLsn(
                                Lsn.valueOf("00000000000000000000000000000001")))
                .isTrue();
    }
}
