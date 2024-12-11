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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.utils.DataTypeUtils;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.kafka.json.TableSchemaInfo;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.lang.String.format;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.AFTER;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.BEFORE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.OPERATION;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumPayload.SOURCE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumSource.DATABASE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumSource.TABLE;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumStruct.PAYLOAD;
import static org.apache.flink.cdc.connectors.kafka.json.debezium.DebeziumJsonStruct.DebeziumStruct.SCHEMA;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema from FlinkCDC pipeline internal data structure {@link Event} to Debezium
 * JSON.
 *
 * @see <a href="https://debezium.io/">Debezium</a>
 */
public class DebeziumJsonSerializationSchema implements SerializationSchema<Event> {
    private static final long serialVersionUID = 1L;

    private static final StringData OP_INSERT = StringData.fromString("c"); // insert
    private static final StringData OP_DELETE = StringData.fromString("d"); // delete
    private static final StringData OP_UPDATE = StringData.fromString("u"); // update

    /**
     * A map of {@link TableId} and its {@link SerializationSchema} to serialize Debezium JSON data.
     */
    private final Map<TableId, TableSchemaInfo> jsonSerializers;

    private transient GenericRowData reuseGenericRowData;

    private transient GenericRowData payloadGenericRowData;

    private final TimestampFormat timestampFormat;

    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    private final String mapNullKeyLiteral;

    private final boolean encodeDecimalAsPlainNumber;

    private final ZoneId zoneId;

    private InitializationContext context;

    private final boolean includeSchemaInfo;

    public DebeziumJsonSerializationSchema(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            ZoneId zoneId,
            boolean encodeDecimalAsPlainNumber,
            boolean includeSchemaInfo) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.zoneId = zoneId;
        jsonSerializers = new HashMap<>();
        this.includeSchemaInfo = includeSchemaInfo;
    }

    @Override
    public void open(InitializationContext context) {
        if (includeSchemaInfo) {
            reuseGenericRowData = new GenericRowData(2);
            payloadGenericRowData = new GenericRowData(4);

            reuseGenericRowData.setField(PAYLOAD.getPosition(), payloadGenericRowData);
        } else {
            reuseGenericRowData = new GenericRowData(4);
        }
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
            LogicalType rowType =
                    DataTypeUtils.toFlinkDataType(schema.toRowDataType()).getLogicalType();
            DebeziumJsonRowDataSerializationSchema jsonSerializer =
                    new DebeziumJsonRowDataSerializationSchema(
                            createJsonRowType(fromLogicalToDataType(rowType), includeSchemaInfo),
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            includeSchemaInfo);
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
        BiConsumer<DataChangeEvent, GenericRowData> converter;
        try {
            switch (dataChangeEvent.op()) {
                case INSERT:
                    converter = this::convertInsertEventToRowData;
                    break;
                case DELETE:
                    converter = this::convertDeleteEventToRowData;
                    break;
                case UPDATE:
                case REPLACE:
                    converter = this::convertUpdateEventToRowData;
                    break;
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for OperationType.",
                                    dataChangeEvent.op()));
            }

            GenericRowData genericRowData =
                    includeSchemaInfo ? payloadGenericRowData : reuseGenericRowData;
            converter.accept(dataChangeEvent, genericRowData);

            if (includeSchemaInfo) {
                reuseGenericRowData.setField(
                        SCHEMA.getPosition(), StringData.fromString(dataChangeEvent.getSchema()));
            }

            return jsonSerializers
                    .get(dataChangeEvent.tableId())
                    .getSerializationSchema()
                    .serialize(reuseGenericRowData);

        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize event '%s'.", event), t);
        }
    }

    private void convertInsertEventToRowData(
            DataChangeEvent dataChangeEvent, GenericRowData genericRowData) {
        genericRowData.setField(BEFORE.getPosition(), null);
        genericRowData.setField(
                AFTER.getPosition(),
                jsonSerializers
                        .get(dataChangeEvent.tableId())
                        .getRowDataFromRecordData(dataChangeEvent.after(), false));
        genericRowData.setField(OPERATION.getPosition(), OP_INSERT);
        genericRowData.setField(
                SOURCE.getPosition(),
                GenericRowData.of(
                        StringData.fromString(dataChangeEvent.tableId().getSchemaName()),
                        StringData.fromString(dataChangeEvent.tableId().getTableName())));
    }

    private void convertDeleteEventToRowData(
            DataChangeEvent dataChangeEvent, GenericRowData genericRowData) {
        genericRowData.setField(
                BEFORE.getPosition(),
                jsonSerializers
                        .get(dataChangeEvent.tableId())
                        .getRowDataFromRecordData(dataChangeEvent.before(), false));
        genericRowData.setField(AFTER.getPosition(), null);
        genericRowData.setField(OPERATION.getPosition(), OP_DELETE);
        genericRowData.setField(
                SOURCE.getPosition(),
                GenericRowData.of(
                        StringData.fromString(dataChangeEvent.tableId().getSchemaName()),
                        StringData.fromString(dataChangeEvent.tableId().getTableName())));
    }

    private void convertUpdateEventToRowData(
            DataChangeEvent dataChangeEvent, GenericRowData genericRowData) {
        genericRowData.setField(
                BEFORE.getPosition(),
                jsonSerializers
                        .get(dataChangeEvent.tableId())
                        .getRowDataFromRecordData(dataChangeEvent.before(), false));
        genericRowData.setField(
                AFTER.getPosition(),
                jsonSerializers
                        .get(dataChangeEvent.tableId())
                        .getRowDataFromRecordData(dataChangeEvent.after(), false));
        genericRowData.setField(OPERATION.getPosition(), OP_UPDATE);
        genericRowData.setField(
                SOURCE.getPosition(),
                GenericRowData.of(
                        StringData.fromString(dataChangeEvent.tableId().getSchemaName()),
                        StringData.fromString(dataChangeEvent.tableId().getTableName())));
    }

    /**
     * Refer to <a
     * href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html">Debezium
     * docs</a> for more details.
     */
    private static RowType createJsonRowType(DataType databaseSchema, boolean includeSchemaInfo) {
        DataType payloadRowType =
                DataTypes.ROW(
                        DataTypes.FIELD(BEFORE.getFieldName(), databaseSchema),
                        DataTypes.FIELD(AFTER.getFieldName(), databaseSchema),
                        DataTypes.FIELD(OPERATION.getFieldName(), DataTypes.STRING()),
                        DataTypes.FIELD(
                                SOURCE.getFieldName(),
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                DATABASE.getFieldName(), DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                TABLE.getFieldName(), DataTypes.STRING()))));

        if (includeSchemaInfo) {
            return (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD(SCHEMA.getFieldName(), DataTypes.STRING()),
                                    DataTypes.FIELD(PAYLOAD.getFieldName(), payloadRowType))
                            .getLogicalType();
        } else {
            return (RowType) payloadRowType.getLogicalType();
        }
    }
}
