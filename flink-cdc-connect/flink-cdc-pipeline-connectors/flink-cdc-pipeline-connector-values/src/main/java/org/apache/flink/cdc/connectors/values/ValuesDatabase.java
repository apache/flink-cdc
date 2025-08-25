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

package org.apache.flink.cdc.connectors.values;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.exceptions.UnsupportedSchemaChangeEventException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.values.sink.ValuesDataSink;
import org.apache.flink.cdc.connectors.values.source.ValuesDataSource;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

        private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

        private final boolean materializedInMemory;

        public ValuesMetadataApplier() {
            this(true);
        }

        public ValuesMetadataApplier(boolean materializedInMemory) {
            this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
            this.materializedInMemory = materializedInMemory;
        }

        @Override
        public MetadataApplier setAcceptedSchemaEvolutionTypes(
                Set<SchemaChangeEventType> schemaEvolutionTypes) {
            this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
            return this;
        }

        @Override
        public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
            return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
        }

        @Override
        public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
            return Sets.newHashSet(
                    SchemaChangeEventType.ADD_COLUMN,
                    SchemaChangeEventType.ALTER_COLUMN_TYPE,
                    SchemaChangeEventType.CREATE_TABLE,
                    SchemaChangeEventType.DROP_COLUMN,
                    SchemaChangeEventType.RENAME_COLUMN);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
            if (materializedInMemory) {
                applySchemaChangeEvent(schemaChangeEvent);
            }
        }
    }

    /**
     * apply SchemaChangeEvent to ValuesDatabase and print it out, throw exception if illegal
     * changes occur.
     */
    public static class ErrorOnChangeMetadataApplier implements MetadataApplier {
        private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

        public ErrorOnChangeMetadataApplier() {
            enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        }

        @Override
        public MetadataApplier setAcceptedSchemaEvolutionTypes(
                Set<SchemaChangeEventType> schemaEvolutionTypes) {
            enabledSchemaEvolutionTypes = schemaEvolutionTypes;
            return this;
        }

        @Override
        public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
            return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
        }

        @Override
        public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
            return Collections.singleton(SchemaChangeEventType.CREATE_TABLE);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
                throws SchemaEvolveException {
            if (schemaChangeEvent instanceof CreateTableEvent) {
                TableId tableId = schemaChangeEvent.tableId();
                if (!globalTables.containsKey(tableId)) {
                    globalTables.put(
                            tableId,
                            new ValuesTable(
                                    tableId, ((CreateTableEvent) schemaChangeEvent).getSchema()));
                }
            } else {
                throw new UnsupportedSchemaChangeEventException(
                        schemaChangeEvent,
                        "Rejected schema change event since error.on.schema.change is enabled.",
                        null);
            }
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

    public static void applyTruncateTableEvent(TruncateTableEvent event) {
        ValuesTable table = globalTables.get(event.tableId());
        Preconditions.checkNotNull(table, event.tableId() + " is not existed");
        table.applyTruncateTableEvent(event);
        LOG.info("apply TruncateTableEvent: " + event);
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
        } else if (event instanceof DropTableEvent) {
            globalTables.remove(tableId);
        } else if (event instanceof TruncateTableEvent) {
            if (globalTables.containsKey(tableId)) {
                ValuesTable table = globalTables.get(event.tableId());
                table.applyTruncateTableEvent((TruncateTableEvent) event);
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
        private final Map<String, Map<String, RecordData>> records;

        private final LinkedList<Column> columns;

        private final LinkedList<String> columnNames;

        private final List<String> primaryKeys;

        // indexes of primaryKeys in columns
        private final List<Integer> primaryKeyIndexes;

        public ValuesTable(TableId tableId, Schema schema) {
            this.tableId = tableId;
            this.lock = new Object();
            this.columns = new LinkedList<>(schema.getColumns());
            this.columnNames = new LinkedList<>(schema.getColumnNames());
            this.records = new HashMap<>();
            this.primaryKeys = new LinkedList<>(schema.primaryKeys());
            this.primaryKeyIndexes = new ArrayList<>();
            updatePrimaryKeyIndexes();
        }

        public List<String> getResult() {
            List<String> results = new ArrayList<>();
            synchronized (lock) {
                List<RecordData.FieldGetter> fieldGetters = SchemaUtils.createFieldGetters(columns);
                records.forEach(
                        (key, record) -> {
                            StringBuilder stringBuilder = new StringBuilder(tableId.toString());
                            stringBuilder.append(":");
                            for (int i = 0; i < columns.size(); i++) {
                                Column column = columns.get(i);
                                RecordData.FieldGetter fieldGetter = fieldGetters.get(i);
                                stringBuilder
                                        .append(column.getName())
                                        .append("=")
                                        .append(
                                                Optional.ofNullable(record.get(column.getName()))
                                                        .map(fieldGetter::getFieldOrNull)
                                                        .orElse(""))
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
            Map<String, RecordData> record = new HashMap<>();
            for (int i = 0; i < recordData.getArity(); i++) {
                record.put(columns.get(i).getName(), recordData);
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
            Preconditions.checkArgument(!primaryKeys.isEmpty(), "primaryKeys couldn't be empty");
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
                            columnNames.addFirst(columnWithPosition.getAddColumn().getName());
                            break;
                        }
                    case LAST:
                        {
                            columns.addLast(columnWithPosition.getAddColumn());
                            columnNames.addLast(columnWithPosition.getAddColumn().getName());
                            break;
                        }
                    case BEFORE:
                        {
                            int index =
                                    columnNames.indexOf(columnWithPosition.getExistedColumnName());
                            columns.add(index, columnWithPosition.getAddColumn());
                            columnNames.add(index, columnWithPosition.getAddColumn().getName());
                            break;
                        }
                    case AFTER:
                        {
                            int index =
                                    columnNames.indexOf(columnWithPosition.getExistedColumnName());
                            columns.add(index + 1, columnWithPosition.getAddColumn());
                            columnNames.add(index + 1, columnWithPosition.getAddColumn().getName());
                            break;
                        }
                }
            }
        }

        private void applyDropColumnEvent(DropColumnEvent event) {
            for (String columnName : event.getDroppedColumnNames()) {
                if (!removeColumn(columnName)) {
                    throw new IllegalArgumentException(columnName + " is not existed");
                }
                records.forEach((key, record) -> record.remove(columnName));
            }
        }

        private boolean removeColumn(String columnName) {
            int index = columnNames.indexOf(columnName);
            if (index == -1) {
                return false;
            }

            return Objects.nonNull(columnNames.remove(index))
                    && Objects.nonNull(columns.remove(index));
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
                                        columnNames.set(i, afterName);
                                    }
                                }

                                records.forEach(
                                        (key, record) -> {
                                            if (record.containsKey(beforeName)) {
                                                RecordData value = record.get(beforeName);
                                                record.remove(beforeName);
                                                record.put(afterName, value);
                                            }
                                        });
                            });
        }

        private void applyTruncateTableEvent(TruncateTableEvent event) {
            records.clear();
        }
    }
}
