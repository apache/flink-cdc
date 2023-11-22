/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DorisDatabase {
    private static final Logger LOG = LoggerFactory.getLogger(DorisDatabase.class);

    public static final Map<TableId, DorisTable> globalTables = new ConcurrentHashMap<>();

    public static void applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        if (event instanceof CreateTableEvent) {
            if (!globalTables.containsKey(event.tableId())) {
                globalTables.put(event.tableId(), new DorisDatabase.DorisTable(event.tableId(), ((CreateTableEvent) event).getSchema()));
            }
        } else {
            DorisTable table = globalTables.get(event.tableId());
            Preconditions.checkNotNull(table, event.tableId() + " is not existed");
            table.applySchemaChangeEvent(event);
        }
    }

    public static Schema getTableSchema(TableId tableId) {
        DorisTable table = globalTables.get(tableId);
        Preconditions.checkNotNull(table, tableId + " is not existed");
        Schema.Builder builder = Schema.newBuilder();
        for (Column column : table.columns) {
            builder.physicalColumn(column.getName(), column.getType());
        }
        return builder.build();
    }

    public static void clear() {
        globalTables.clear();
    }

    /** maintain the columns, primaryKeys of a specific table in memory. */
    static class DorisTable {

        private final Object lock;

        private final TableId tableId;

        private final LinkedList<Column> columns;

        public LinkedList<Column> getColumns() {
            return columns;
        }

        public DorisTable(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.lock = new Object();
            this.columns = new LinkedList<>(schema.getColumns());
        }

        public void applySchemaChangeEvent(SchemaChangeEvent event) {
            synchronized (lock) {
                if (event instanceof AddColumnEvent) {
                    applyAddColumnEvent((AddColumnEvent) event);
                } else if (event instanceof DropColumnEvent) {
                    applyDropColumnEvent((DropColumnEvent) event);
                } else if (event instanceof RenameColumnEvent) {
                    applyRenameColumnEvent((RenameColumnEvent) event);
                } else if (event instanceof AlterColumnTypeEvent) {
                    applyAlterColumnTypeEvent((AlterColumnTypeEvent) event);
                }
            }
        }

        private void applyAlterColumnTypeEvent(AlterColumnTypeEvent event) {
            event.getTypeMapping()
                    .forEach(
                            (columnName, columnType) -> {
                                for (int i = 0; i < columns.size(); i++) {
                                    if (columns.get(i).getName().equals(columnName)) {
                                        columns.set(
                                                i, Column.physicalColumn(columnName, columnType));
                                    }
                                }
                            });
        }

        private void applyAddColumnEvent(AddColumnEvent event) {
            for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
                if (columns.contains(columnWithPosition.getAddColumn())) {
                    throw new IllegalArgumentException(
                            columnWithPosition.getAddColumn().getName() + " is already existed");
                }
                switch (columnWithPosition.getPosition()) {
                    case FIRST:
                    {
                        columns.addFirst(columnWithPosition.getAddColumn());
                        break;
                    }
                    case LAST:
                    {
                        columns.addLast(columnWithPosition.getAddColumn());
                        break;
                    }
                    case BEFORE:
                    {
                        int index = columns.indexOf(columnWithPosition.getExistingColumn());
                        columns.add(index, columnWithPosition.getExistingColumn());
                        break;
                    }
                    case AFTER:
                    {
                        int index = columns.indexOf(columnWithPosition.getExistingColumn());
                        columns.add(index + 1, columnWithPosition.getExistingColumn());
                        break;
                    }
                }
            }
        }

        private void applyDropColumnEvent(DropColumnEvent event) {
            for (Column column : event.getDroppedColumns()) {
                if (!columns.remove(column)) {
                    throw new IllegalArgumentException(column.getName() + " is not existed");
                }
            }
        }

        private void applyRenameColumnEvent(RenameColumnEvent event) {
            event.getNameMapping()
                    .forEach(
                            (beforeName, afterName) -> {
                                for (int i = 0; i < columns.size(); i++) {
                                    if (columns.get(i).getName().equals(beforeName)) {
                                        Column column = columns.get(i);
                                        columns.set(
                                                i,
                                                Column.physicalColumn(afterName, column.getType()));
                                    }
                                }
                            });
        }
    }
}
