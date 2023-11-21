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

package com.ververica.cdc.connectors.values;

import com.ververica.cdc.common.annotation.Internal;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.source.MetadataAccessor;
import com.ververica.cdc.common.utils.Preconditions;
import com.ververica.cdc.connectors.values.sink.ValuesDataSink;
import com.ververica.cdc.connectors.values.source.ValuesDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * An auxiliary class for simulating a database, use for {@link ValuesDataSource} and {@link
 * ValuesDataSink}.
 */
@Internal
public class ValuesDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(ValuesDatabase.class);

    private static final Map<TableId, ValuesTable> globalTables = new ConcurrentHashMap<>();

    /**
     * apply SchemaChangeEvent to ValuesDatabase and print it out, throw exception if illegal
     * changes occur.
     */
    public static class ValuesMetadataApplier implements MetadataApplier {

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            applySchemaChangeEvent(schemaChangeEvent);
        }
    }

    /** provide namespace/schema/table lists for {@link ValuesDataSource}. */
    public static class ValuesMetadataAccessor implements MetadataAccessor {

        @Override
        public List<String> listNamespaces() throws UnsupportedOperationException {
            Set<String> namespaces = new HashSet<>();
            globalTables.keySet().forEach((tableId) -> namespaces.add(tableId.getNamespace()));
            return new ArrayList<>(namespaces);
        }

        @Override
        public List<String> listSchemas(@Nullable String namespace)
                throws UnsupportedOperationException {
            Set<String> schemas = new HashSet<>();
            globalTables.keySet().stream()
                    .filter(
                            (tableId) -> {
                                if (namespace == null) {
                                    return tableId.getNamespace() == null;
                                } else {
                                    return namespace.equals(tableId.getNamespace());
                                }
                            })
                    .forEach(tableId -> schemas.add(tableId.getSchemaName()));
            return new ArrayList<>(schemas);
        }

        @Override
        public List<TableId> listTables(@Nullable String namespace, @Nullable String schemaName) {
            return globalTables.keySet().stream()
                    .filter(
                            (tableId) -> {
                                if (namespace == null) {
                                    if (schemaName == null) {
                                        return tableId.getNamespace() == null
                                                && tableId.getSchemaName() == null;
                                    } else {
                                        return tableId.getNamespace() == null
                                                && schemaName.equals(tableId.getSchemaName());
                                    }
                                } else {
                                    // schemaName will not be null
                                    return namespace.equals(tableId.getNamespace())
                                            && schemaName.equals(tableId.getSchemaName());
                                }
                            })
                    .collect(Collectors.toList());
        }

        @Override
        public Schema getTableSchema(TableId tableId) {
            return ValuesDatabase.getTableSchema(tableId);
        }
    }

    public static Schema getTableSchema(TableId tableId) {
        ValuesTable table = globalTables.get(tableId);
        Preconditions.checkNotNull(table, tableId + " is not existed");
        Schema.Builder builder = Schema.newBuilder();
        for (Column column : table.columns) {
            builder.physicalColumn(column.getName(), column.getType());
        }
        return builder.primaryKey(table.primaryKeys).build();
    }

    public static void applyDataChangeEvent(DataChangeEvent event) {
        ValuesTable table = globalTables.get(event.tableId());
        Preconditions.checkNotNull(table, event.tableId() + " is not existed");
        table.applyDataChangeEvent(event);
        LOG.info("apply DataChangeEvent: " + event);
    }

    public static void applySchemaChangeEvent(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        if (event instanceof CreateTableEvent) {
            if (!globalTables.containsKey(tableId)) {
                globalTables.put(
                        tableId, new ValuesTable(tableId, ((CreateTableEvent) event).getSchema()));
            }
        } else {
            ValuesTable table = globalTables.get(event.tableId());
            Preconditions.checkNotNull(table, event.tableId() + " is not existed");
            table.applySchemaChangeEvent(event);
        }
    }

    public static List<String> getAllResults() {
        List<String> results = new ArrayList<>();
        globalTables.values().forEach(valuesTable -> results.addAll(valuesTable.getResult()));
        return results;
    }

    public static List<String> getResults(TableId tableId) {
        ValuesTable table = globalTables.get(tableId);
        Preconditions.checkNotNull(table, tableId + " is not existed");
        return table.getResult();
    }

    public static void clear() {
        globalTables.clear();
    }

    /** maintain the columns, primaryKeys and records of a specific table in memory. */
    private static class ValuesTable {

        private final Object lock;

        private final TableId tableId;

        // [primaryKeys, [column_name, column_value]]
        private final Map<String, Map<String, String>> records;

        private final LinkedList<Column> columns;

        private final List<String> primaryKeys;

        // indexes of primaryKeys in columns
        private final List<Integer> primaryKeyIndexes;

        public ValuesTable(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.lock = new Object();
            this.columns = new LinkedList<>(schema.getColumns());
            this.records = new HashMap<>();
            this.primaryKeys = new LinkedList<>(schema.primaryKeys());
            this.primaryKeyIndexes = new ArrayList<>();
            updatePrimaryKeyIndexes();
        }

        public List<String> getResult() {
            List<String> results = new ArrayList<>();
            synchronized (lock) {
                records.forEach(
                        (key, record) -> {
                            StringBuilder stringBuilder = new StringBuilder(tableId.toString());
                            stringBuilder.append(":");
                            for (Column column : columns) {
                                stringBuilder
                                        .append(column.getName())
                                        .append("=")
                                        .append(record.getOrDefault(column.getName(), ""))
                                        .append(";");
                            }
                            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                            results.add(stringBuilder.toString());
                        });
            }
            return results;
        }

        public void applyDataChangeEvent(DataChangeEvent event) {
            synchronized (lock) {
                switch (event.op()) {
                    case INSERT:
                        {
                            insert(event.after());
                            break;
                        }
                    case DELETE:
                        {
                            delete(event.before());
                            break;
                        }
                    case REPLACE:
                    case UPDATE:
                        {
                            update(event.before(), event.after());
                            break;
                        }
                }
            }
        }

        private void delete(RecordData recordData) {
            records.remove(buildPrimaryKeyStr(recordData));
        }

        private void insert(RecordData recordData) {
            String primaryKey = buildPrimaryKeyStr(recordData);
            Map<String, String> record = new HashMap<>();
            for (int i = 0; i < recordData.getArity(); i++) {
                record.put(columns.get(i).getName(), recordData.getString(i).toString());
            }
            records.put(primaryKey, record);
        }

        private void update(RecordData beforeRecordData, RecordData afterRecordData) {
            insert(afterRecordData);
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
                updatePrimaryKeyIndexes();
            }
        }

        private void updatePrimaryKeyIndexes() {
            Preconditions.checkArgument(primaryKeys.size() > 0, "primaryKeys couldn't be empty");
            primaryKeyIndexes.clear();
            for (String primaryKey : primaryKeys) {
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).getName().equals(primaryKey)) {
                        primaryKeyIndexes.add(i);
                    }
                }
            }
            primaryKeyIndexes.sort(Comparator.naturalOrder());
        }

        private String buildPrimaryKeyStr(RecordData recordData) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Integer primaryKeyIndex : primaryKeyIndexes) {
                stringBuilder.append(recordData.getString(primaryKeyIndex).toString()).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            return stringBuilder.toString();
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
                records.forEach((key, record) -> record.remove(column.getName()));
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

                                records.forEach(
                                        (key, record) -> {
                                            if (record.containsKey(beforeName)) {
                                                String value = record.get(beforeName);
                                                record.remove(beforeName);
                                                record.put(afterName, value);
                                            }
                                        });
                            });
        }
    }
}
