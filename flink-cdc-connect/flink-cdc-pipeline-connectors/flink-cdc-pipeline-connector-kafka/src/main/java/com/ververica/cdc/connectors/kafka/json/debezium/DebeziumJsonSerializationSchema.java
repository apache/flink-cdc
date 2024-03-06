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

package com.ververica.cdc.connectors.kafka.json.debezium;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.common.event.CreateTableEvent;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.Event;
import com.ververica.cdc.common.event.SchemaChangeEvent;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.schema.Schema;
import com.ververica.cdc.common.types.utils.DataTypeUtils;
import com.ververica.cdc.common.utils.SchemaUtils;
import com.ververica.cdc.connectors.kafka.json.TableSchemaInfo;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
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

    private final TimestampFormat timestampFormat;

    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    private final String mapNullKeyLiteral;

    private final boolean encodeDecimalAsPlainNumber;

    private final ZoneId zoneId;

    private InitializationContext context;

    public DebeziumJsonSerializationSchema(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            boolean encodeDecimalAsPlainNumber) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
        this.encodeDecimalAsPlainNumber = encodeDecimalAsPlainNumber;
        this.zoneId = ZoneId.systemDefault();
        jsonSerializers = new HashMap<>();
    }

    @Override
    public void open(InitializationContext context) {
        reuseGenericRowData = new GenericRowData(3);
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
            JsonRowDataSerializationSchema jsonSerializer =
                    new JsonRowDataSerializationSchema(
                            createJsonRowType(fromLogicalToDataType(rowType)),
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber);
            try {
                jsonSerializer.open(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            jsonSerializers.put(
                    schemaChangeEvent.tableId(),
                    new TableSchemaInfo(schema, jsonSerializer, zoneId));
            return null;
        }

        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        try {
            switch (dataChangeEvent.op()) {
                case INSERT:
                    reuseGenericRowData.setField(0, null);
                    reuseGenericRowData.setField(
                            1,
                            jsonSerializers
                                    .get(dataChangeEvent.tableId())
                                    .getRowDataFromRecordData(dataChangeEvent.after()));
                    reuseGenericRowData.setField(2, OP_INSERT);
                    return jsonSerializers
                            .get(dataChangeEvent.tableId())
                            .getSerializationSchema()
                            .serialize(reuseGenericRowData);
                case DELETE:
                    reuseGenericRowData.setField(
                            0,
                            jsonSerializers
                                    .get(dataChangeEvent.tableId())
                                    .getRowDataFromRecordData(dataChangeEvent.before()));
                    reuseGenericRowData.setField(1, null);
                    reuseGenericRowData.setField(2, OP_DELETE);
                    return jsonSerializers
                            .get(dataChangeEvent.tableId())
                            .getSerializationSchema()
                            .serialize(reuseGenericRowData);
                case UPDATE:
                case REPLACE:
                    reuseGenericRowData.setField(
                            0,
                            jsonSerializers
                                    .get(dataChangeEvent.tableId())
                                    .getRowDataFromRecordData(dataChangeEvent.before()));
                    reuseGenericRowData.setField(
                            1,
                            jsonSerializers
                                    .get(dataChangeEvent.tableId())
                                    .getRowDataFromRecordData(dataChangeEvent.after()));
                    reuseGenericRowData.setField(2, OP_UPDATE);
                    return jsonSerializers
                            .get(dataChangeEvent.tableId())
                            .getSerializationSchema()
                            .serialize(reuseGenericRowData);
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for OperationType.",
                                    dataChangeEvent.op()));
            }
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize event '%s'.", event), t);
        }
    }

    private static RowType createJsonRowType(DataType databaseSchema) {
        // Debezium JSON contains some other information, e.g. "source", "ts_ms"
        // but we don't need them.
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("before", databaseSchema),
                                DataTypes.FIELD("after", databaseSchema),
                                DataTypes.FIELD("op", DataTypes.STRING()))
                        .getLogicalType();
    }
}
