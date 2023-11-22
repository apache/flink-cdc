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
import com.ververica.cdc.common.sink.MetadataApplier;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.catalog.doris.DorisType;
import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.catalog.doris.TableSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DorisMetadataApplier implements MetadataApplier {
    private static final Logger LOG = LoggerFactory.getLogger(DorisMetadataApplier.class);
    private DorisOptions dorisOptions;
    private SchemaChangeManager schemaChangeManager;

    public DorisMetadataApplier(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try{
            //refresh meta in memory
            DorisDatabase.applySchemaChangeEvent(event);
            //send schema change op to doris
            if (event instanceof CreateTableEvent){
                applyCreateTableEvent((CreateTableEvent) event);
            } else if (event instanceof AddColumnEvent) {
                applyAddColumnEvent((AddColumnEvent) event);
            } else if (event instanceof DropColumnEvent) {
                applyDropColumnEvent((DropColumnEvent) event);
            } else if (event instanceof RenameColumnEvent) {
                applyRenameColumnEvent((RenameColumnEvent) event);
            } else if (event instanceof AlterColumnTypeEvent) {
                LOG.warn("AlterColumnType is not supported, skip.");
            }
        }catch (Exception ex){
            //Catch exceptions to avoid affecting the main process
            LOG.error("Schema change error, skip {}", event);
        }
    }

    private void applyCreateTableEvent(CreateTableEvent event) throws IOException, IllegalArgumentException {
        Schema schema = event.getSchema();
        TableId tableId = event.tableId();
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTable(tableId.getTableName());
        tableSchema.setDatabase(tableId.getSchemaName());
        tableSchema.setFields(buildFields(schema));
        tableSchema.setDistributeKeys(buildDistributeKeys(schema));

        if(CollectionUtil.isNullOrEmpty(schema.primaryKeys())){
            tableSchema.setModel(DataModel.DUPLICATE);
        }else{
            tableSchema.setKeys(schema.primaryKeys());
            tableSchema.setModel(DataModel.UNIQUE);
        }
        schemaChangeManager.createTable(tableSchema);
    }

    private Map<String, FieldSchema> buildFields(Schema schema){
        Map<String, FieldSchema> fieldSchemaMap = new HashMap<>();
        List<String> columnNameList = schema.getColumnNames();
        for(String columnName : columnNameList){
            Column column = schema.getColumn(columnName).get();
            String dorisTypeStr = convertToDorisType(column.getType());
            fieldSchemaMap.put(column.getName(), new FieldSchema(column.getName(), dorisTypeStr, column.getComment()));
        }
        return fieldSchemaMap;
    }

    private List<String> buildDistributeKeys(Schema schema){
        if(!CollectionUtil.isNullOrEmpty(schema.primaryKeys())){
            return schema.primaryKeys();
        }
        if(!CollectionUtil.isNullOrEmpty(schema.getColumnNames())){
            return Collections.singletonList(schema.getColumnNames().get(0));
        }
        return new ArrayList<>();
    }

    private void applyAddColumnEvent(AddColumnEvent event) throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
        for(AddColumnEvent.ColumnWithPosition col: addedColumns){
            Column column = col.getAddColumn();
            String typeString = convertToDorisType(column.getType());
            FieldSchema addFieldSchema = new FieldSchema(column.getName(), typeString, column.getComment());
            schemaChangeManager.addColumn(tableId.getSchemaName(), tableId.getTableName(), addFieldSchema);
        }
    }

    private  void applyDropColumnEvent(DropColumnEvent event) throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        List<Column> droppedColumns = event.getDroppedColumns();
        for(Column col : droppedColumns){
            schemaChangeManager.dropColumn(tableId.getSchemaName(), tableId.getTableName(), col.getName());
        }
    }

    private  void applyRenameColumnEvent(RenameColumnEvent event) throws IOException, IllegalArgumentException {
        TableId tableId = event.tableId();
        Map<String, String> nameMapping = event.getNameMapping();
        for(Map.Entry<String, String> entry : nameMapping.entrySet()){
            schemaChangeManager.renameColumn(tableId.getSchemaName(), tableId.getTableName(), entry.getKey(), entry.getValue());
        }
    }

    /**
     * convert datatype to doris type for add column event
     */
    private String convertToDorisType(DataType type) {
        int length = DataTypes.getLength(type).orElse(0);
        int precision = DataTypes.getPrecision(type).orElse(0);
        int scale = DataTypes.getScale(type).orElse(0);
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return DorisType.BOOLEAN;
            case TINYINT:
                return DorisType.TINYINT;
            case SMALLINT:
                return DorisType.SMALLINT;
            case INTEGER:
                return DorisType.INT;
            case BIGINT:
                return DorisType.BIGINT;
            case FLOAT:
                return DorisType.FLOAT;
            case DOUBLE:
                return DorisType.DOUBLE;
            case DECIMAL:
                return precision > 0 && precision <= 38
                        ? String.format("%s(%s,%s)", DorisType.DECIMAL_V3, length, Math.max(scale, 0))
                        : DorisType.STRING;
            case DATE:
                return DorisType.DATE_V2;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return String.format("%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
            case CHAR:
            case VARCHAR:
                return length * 4 > 65533 ? DorisType.STRING : String.format("%s(%s)", DorisType.VARCHAR, length * 4);
            case BINARY:
            case VARBINARY:
            case ARRAY:
            case MAP:
            case TIME_WITHOUT_TIME_ZONE:
                return DorisType.STRING;
            case ROW:
                return DorisType.JSONB;
            default:
                throw new UnsupportedOperationException("Unsupported Flink Type: " + type);
        }
    }
}
