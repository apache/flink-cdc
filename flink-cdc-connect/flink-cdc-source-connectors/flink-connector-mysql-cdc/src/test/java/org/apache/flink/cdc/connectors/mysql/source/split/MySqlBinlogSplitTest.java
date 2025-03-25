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

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit tests for {@link MySqlBinlogSplit}. */
class MySqlBinlogSplitTest {

    @Test
    void filterOutdatedSplitInfos() {
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();

        // mock table1
        TableId tableId1 = new TableId("catalog1", null, "table1");

        TableChanges.TableChange tableChange1 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog1.table1")));

        // mock table2
        TableId tableId2 = new TableId("catalog2", null, "table2");

        TableChanges.TableChange tableChange2 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog2.table2")));
        tableSchemas.put(tableId1, tableChange1);
        tableSchemas.put(tableId2, tableChange2);
        MySqlBinlogSplit binlogSplit =
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofLatest(),
                        null,
                        new ArrayList<>(),
                        tableSchemas,
                        0,
                        false);
        String expectedTables = "[catalog1.table1, catalog2.table2]";
        Assertions.assertThat(binlogSplit.getTables()).isEqualTo(expectedTables);

        // case 1: only include table1
        Tables.TableFilter currentTableFilter = tableId -> tableId.table().equals("table1");

        MySqlBinlogSplit mySqlBinlogSplit =
                MySqlBinlogSplit.filterOutdatedSplitInfos(binlogSplit, currentTableFilter);
        Map<TableId, TableChanges.TableChange> filterTableSchemas =
                mySqlBinlogSplit.getTableSchemas();
        Assertions.assertThat(filterTableSchemas).hasSize(1).containsEntry(tableId1, tableChange1);
        String expectedTables1 = "[catalog1.table1]";
        Assertions.assertThat(mySqlBinlogSplit.getTables()).isEqualTo(expectedTables1);

        // case 2: include all tables
        currentTableFilter = tableId -> tableId.table().startsWith("table");

        mySqlBinlogSplit =
                MySqlBinlogSplit.filterOutdatedSplitInfos(binlogSplit, currentTableFilter);
        filterTableSchemas = mySqlBinlogSplit.getTableSchemas();
        Assertions.assertThat(filterTableSchemas)
                .hasSize(2)
                .containsEntry(tableId1, tableChange1)
                .containsEntry(tableId2, tableChange2);
        String expectedTables2 = "[catalog1.table1, catalog2.table2]";
        Assertions.assertThat(mySqlBinlogSplit.getTables()).isEqualTo(expectedTables2);
    }

    @Test
    void testTruncatedTablesForLog() {
        Map<TableId, TableChanges.TableChange> tableSchemas = new HashMap<>();

        // mock table1
        TableId tableId1 = new TableId("catalog1", null, "table1");

        TableChanges.TableChange tableChange1 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog1.table1")));

        // mock table2
        TableId tableId2 = new TableId("catalog2", null, "table2");

        TableChanges.TableChange tableChange2 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog2.table2")));

        // mock table3
        TableId tableId3 = new TableId("catalog3", null, "table3");

        TableChanges.TableChange tableChange3 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog3.table3")));

        // mock table4
        TableId tableId4 = new TableId("catalog4", null, "table4");

        TableChanges.TableChange tableChange4 =
                new TableChanges.TableChange(
                        TableChanges.TableChangeType.CREATE,
                        new MockTable(TableId.parse("catalog4.table4")));

        tableSchemas.put(tableId1, tableChange1);
        tableSchemas.put(tableId2, tableChange2);
        tableSchemas.put(tableId3, tableChange3);
        tableSchemas.put(tableId4, tableChange4);
        MySqlBinlogSplit binlogSplit =
                new MySqlBinlogSplit(
                        "binlog-split",
                        BinlogOffset.ofLatest(),
                        null,
                        new ArrayList<>(),
                        tableSchemas,
                        0,
                        false);
        String expectedTables = "[catalog1.table1, catalog2.table2, catalog3.table3]";
        Assertions.assertThat(binlogSplit.getTables()).isEqualTo(expectedTables);
    }

    /** A mock implementation for {@link Table} which is used for unit tests. */
    private static class MockTable implements Table {
        private final TableId tableId;

        public MockTable(TableId tableId) {
            this.tableId = tableId;
        }

        @Override
        public TableId id() {
            return tableId;
        }

        @Override
        public List<String> primaryKeyColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<String> retrieveColumnNames() {
            return Collections.emptyList();
        }

        @Override
        public List<Column> columns() {
            return Collections.emptyList();
        }

        @Override
        public Column columnWithName(String name) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public String defaultCharsetName() {
            return "UTF-8";
        }

        @Override
        public String comment() {
            return null;
        }

        @Override
        public TableEditor edit() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
