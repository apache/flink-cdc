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

import org.apache.flink.util.CollectionUtil;

import com.ververica.cdc.common.configuration.Configuration;
import com.ververica.cdc.common.event.AddColumnEvent;
import com.ververica.cdc.common.event.AlterColumnTypeEvent;
import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DropColumnEvent;
import com.ververica.cdc.common.event.RenameColumnEvent;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.DataTypeChecks;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import com.ververica.cdc.common.types.utils.DataTypeUtils;
import org.apache.doris.flink.catalog.DorisTypeMapper;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.ververica.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PROPERTIES_PREFIX;

/** Supports {@link DorisDataSink} to schema evolution. */
public class DorisMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);
    private DorisOptions dorisOptions;
    private SchemaChangeManager schemaChangeManager;
    private Configuration config;

    public DorisMetadataApplier(DorisOptions dorisOptions, Configuration config) {
        this.dorisOptions = dorisOptions;
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.config = config;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            // send schema change op to doris
            if (event instanceof CreateTableEvent) {
                applyCreateTableEvent((CreateTableEvent) event);
            } else if (event instanceof AddColumnEvent) {
                applyAddColumnEvent((AddColumnEvent) event);
            } else if (event instanceof DropColumnEvent) {
                applyDropColumnEvent((DropColumnEvent) event);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumnEvent((RenameColumnEvent) event);
            } else if (event instanceof AlterColumnTypeEvent) {
                throw new RuntimeException("Unsupport schema change event, " + event);
            }
        } catch (Exception ex) {
            throw new RuntimeException(
                    "Failed to schema change, " + event + ", reason: " + ex.getMessage());
        }
    }

    private void applyCreateTableEvent(CreateTableEvent event)
            throws IOException, IllegalArgumentException {
        Schema schema = event.getSchema();
        TableId tableId = event.tableId();
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTable(tableId.getTableName());
        tableSchema.setDatabase(tableId.getSchemaName());
        tableSchema.setFields(buildFields(schema));
        tableSchema.setDistributeKeys(buildDistributeKeys(schema));

        if (CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            tableSchema.setModel(DataModel.DUPLICATE);
        } else {
            tableSchema.setKeys(schema.primaryKeys());
            tableSchema.setModel(DataModel.UNIQUE);
        }

        Map<String, String> tableProperties =
                DorisDataSinkOptions.getPropertiesByPrefix(config, TABLE_CREATE_PROPERTIES_PREFIX);
        tableSchema.setProperties(tableProperties);
        schemaChangeManager.createTable(tableSchema);
    }

    private Map<String, FieldSchema> buildFields(Schema schema) {
        // Guaranteed the order of column
        Map<String, FieldSchema> fieldSchemaMap = new LinkedHashMap<>();
        List<String> columnNameList = schema.getColumnNames();
        for (String columnName : columnNameList) {
            Column column = schema.getColumn(columnName).get();
            String typeString;
            if (column.getType() instanceof LocalZonedTimestampType
                    || column.getType() instanceof TimestampType
                    || column.getType() instanceof ZonedTimestampType) {
                int precision = DataTypeChecks.getPrecision(column.getType());
                typeString =
                        String.format("%s(%s)", "DATETIMEV2", Math.min(Math.max(precision, 0), 6));
            } else {
                typeString =
                        DorisTypeMapper.toDorisType(
                                DataTypeUtils.toFlinkDataType(column.getType()));
            }
            fieldSchemaMap.put(
                    column.getName(),
                    new FieldSchema(column.getName(), typeString, column.getComment()));
        }
        return fieldSchemaMap;
    }

    private List<String> buildDistributeKeys(Schema schema) {
        if (!CollectionUtil.isNullOrEmpty(schema.primaryKeys())) {
            return schema.primaryKeys();
        }
        if (!CollectionUtil.isNullOrEmpty(schema.getColumnNames())) {
            return Collections.singletonList(schema.getColumnNames().get(0));
        }
        return new ArrayList<>();
    }

    private void applyAddColumnEvent(AddColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
        for (AddColumnEvent.ColumnWithPosition col : addedColumns) {
            Column column = col.getAddColumn();
            String typeString;
            if (column.getType() instanceof LocalZonedTimestampType
                    || column.getType() instanceof TimestampType
                    || column.getType() instanceof ZonedTimestampType) {
                int precision = DataTypeChecks.getPrecision(column.getType());
                typeString =
                        String.format("%s(%s)", "DATETIMEV2", Math.min(Math.max(precision, 0), 6));
            } else {
                typeString =
                        DorisTypeMapper.toDorisType(
                                DataTypeUtils.toFlinkDataType(column.getType()));
            }
            FieldSchema addFieldSchema =
                    new FieldSchema(column.getName(), typeString, column.getComment());
            schemaChangeManager.addColumn(
                    tableId.getSchemaName(), tableId.getTableName(), addFieldSchema);
        }
    }

    private void applyDropColumnEvent(DropColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<Column> droppedColumns = event.getDroppedColumns();
        for (Column col : droppedColumns) {
            schemaChangeManager.dropColumn(
                    tableId.getSchemaName(), tableId.getTableName(), col.getName());
        }
    }

    private void applyRenameColumnEvent(RenameColumnEvent event)
            throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        Map<String, String> nameMapping = event.getNameMapping();
        for (Map.Entry<String, String> entry : nameMapping.entrySet()) {
            schemaChangeManager.renameColumn(
                    tableId.getSchemaName(),
                    tableId.getTableName(),
                    entry.getKey(),
                    entry.getValue());
        }
    }
}
