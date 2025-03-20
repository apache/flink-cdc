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

package org.apache.flink.cdc.connectors.kafka.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataField;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaInfo;
import org.apache.flink.cdc.connectors.kafka.utils.JsonRowDataSerializationSchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

/** A {@link SerializationSchema} to convert {@link Event} into byte of json format. */
public class JsonSerializationSchema implements SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    /**
     * A map of {@link TableId} and its {@link SerializationSchema} to serialize Debezium JSON data.
     */
    private final Map<TableId, TableSchemaInfo> jsonSerializers;

    private final TimestampFormat timestampFormat;

    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    private final String mapNullKeyLiteral;

    private final boolean encodeDecimalAsPlainNumber;

    private final boolean ignoreNullFields;

    private final ZoneId zoneId;

    private InitializationContext context;

    public JsonSerializationSchema(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            ZoneId zoneId,
            boolean encodeDecimalAsPlainNumber,
            boolean ignoreNullFields) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.zoneId = zoneId;
        jsonSerializers = new HashMap<>();
        this.ignoreNullFields = ignoreNullFields;
    }

    @Override
    public void open(InitializationContext context) {
        this.context = context;
    }

    @Override
    public byte[] serialize(Event event) {
        if (event instanceof SchemaChangeEvent) {
            Schema schema;
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) event;
            if (event instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) event;
                schema = createTableEvent.getSchema();
            } else {
                schema =
                        SchemaUtils.applySchemaChangeEvent(
                                jsonSerializers.get(schemaChangeEvent.tableId()).getSchema(),
                                schemaChangeEvent);
            }
            JsonRowDataSerializationSchema jsonSerializer = buildSerializationForPrimaryKey(schema);
            try {
                jsonSerializer.open(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            jsonSerializers.put(
                    schemaChangeEvent.tableId(),
                    new TableSchemaInfo(
                            schemaChangeEvent.tableId(), schema, jsonSerializer, zoneId));
            return null;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        RecordData recordData =
                dataChangeEvent.op().equals(OperationType.DELETE)
                        ? dataChangeEvent.before()
                        : dataChangeEvent.after();
        TableSchemaInfo tableSchemaInfo = jsonSerializers.get(dataChangeEvent.tableId());
        return tableSchemaInfo
                .getSerializationSchema()
                .serialize(tableSchemaInfo.getRowDataFromRecordData(recordData, true));
    }

    private JsonRowDataSerializationSchema buildSerializationForPrimaryKey(Schema schema) {
        DataField[] fields = new DataField[schema.primaryKeys().size() + 1];
        fields[0] = DataTypes.FIELD("TableId", DataTypes.STRING());
        for (int i = 0; i < schema.primaryKeys().size(); i++) {
            Column column = schema.getColumn(schema.primaryKeys().get(i)).get();
            fields[i + 1] = DataTypes.FIELD(column.getName(), column.getType());
        }
        // the row should never be null
        DataType dataType = DataTypes.ROW(fields).notNull();
        LogicalType rowType = DataTypeUtils.toFlinkDataType(dataType).getLogicalType();
        return JsonRowDataSerializationSchemaUtils.createSerializationSchema(
                (RowType) rowType,
                timestampFormat,
                mapNullKeyMode,
                mapNullKeyLiteral,
                encodeDecimalAsPlainNumber,
                ignoreNullFields);
    }
}
