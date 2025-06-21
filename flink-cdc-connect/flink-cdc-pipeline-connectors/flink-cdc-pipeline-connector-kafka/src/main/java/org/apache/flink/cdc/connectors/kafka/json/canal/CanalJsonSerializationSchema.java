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

package org.apache.flink.cdc.connectors.kafka.json.canal;

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
import org.apache.flink.cdc.connectors.kafka.utils.JsonRowDataSerializationSchemaUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.JsonRowDataSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Serialization schema that serializes an object of FlinkCDC pipeline internal data structure
 * {@link Event} into a Canal JSON bytes.
 *
 * @see <a href="https://github.com/alibaba/canal">Alibaba Canal</a>
 */
public class CanalJsonSerializationSchema implements SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    private static final StringData OP_INSERT = StringData.fromString("INSERT");
    private static final StringData OP_DELETE = StringData.fromString("DELETE");
    private static final StringData OP_UPDATE = StringData.fromString("UPDATE");

    private transient GenericRowData reuseGenericRowData;

    /** The serializer to serialize Canal JSON data. */
    private final Map<TableId, TableSchemaInfo> jsonSerializers;

    private final TimestampFormat timestampFormat;

    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;

    private final String mapNullKeyLiteral;

    private final boolean encodeDecimalAsPlainNumber;

    private final boolean ignoreNullFields;

    private final ZoneId zoneId;

    private InitializationContext context;

    public CanalJsonSerializationSchema(
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
        reuseGenericRowData = new GenericRowData(6);
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
                    JsonRowDataSerializationSchemaUtils.createSerializationSchema(
                            createJsonRowType(fromLogicalToDataType(rowType)),
                            timestampFormat,
                            mapNullKeyMode,
                            mapNullKeyLiteral,
                            encodeDecimalAsPlainNumber,
                            ignoreNullFields);
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
        reuseGenericRowData.setField(
                3, StringData.fromString(dataChangeEvent.tableId().getSchemaName()));
        reuseGenericRowData.setField(
                4, StringData.fromString(dataChangeEvent.tableId().getTableName()));
        reuseGenericRowData.setField(
                5,
                new GenericArrayData(
                        jsonSerializers.get(dataChangeEvent.tableId()).getSchema().primaryKeys()
                                .stream()
                                .map(StringData::fromString)
                                .toArray()));
        try {
            switch (dataChangeEvent.op()) {
                case INSERT:
                    reuseGenericRowData.setField(0, null);
                    reuseGenericRowData.setField(
                            1,
                            new GenericArrayData(
                                    new RowData[] {
                                        jsonSerializers
                                                .get(dataChangeEvent.tableId())
                                                .getRowDataFromRecordData(
                                                        dataChangeEvent.after(), false)
                                    }));
                    reuseGenericRowData.setField(2, OP_INSERT);
                    return jsonSerializers
                            .get(dataChangeEvent.tableId())
                            .getSerializationSchema()
                            .serialize(reuseGenericRowData);
                case DELETE:
                    reuseGenericRowData.setField(0, null);
                    reuseGenericRowData.setField(
                            1,
                            new GenericArrayData(
                                    new RowData[] {
                                        jsonSerializers
                                                .get(dataChangeEvent.tableId())
                                                .getRowDataFromRecordData(
                                                        dataChangeEvent.before(), false)
                                    }));
                    reuseGenericRowData.setField(2, OP_DELETE);
                    return jsonSerializers
                            .get(dataChangeEvent.tableId())
                            .getSerializationSchema()
                            .serialize(reuseGenericRowData);
                case UPDATE:
                case REPLACE:
                    reuseGenericRowData.setField(
                            0,
                            new GenericArrayData(
                                    new RowData[] {
                                        jsonSerializers
                                                .get(dataChangeEvent.tableId())
                                                .getRowDataFromRecordData(
                                                        dataChangeEvent.before(), false)
                                    }));
                    reuseGenericRowData.setField(
                            1,
                            new GenericArrayData(
                                    new RowData[] {
                                        jsonSerializers
                                                .get(dataChangeEvent.tableId())
                                                .getRowDataFromRecordData(
                                                        dataChangeEvent.after(), false)
                                    }));
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

    /**
     * Refer to <a
     * href="https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/canal/#available-metadata">Canal
     * | Apache Flink</a> for more details.
     */
    private static RowType createJsonRowType(DataType databaseSchema) {
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD("old", DataTypes.ARRAY(databaseSchema)),
                                DataTypes.FIELD("data", DataTypes.ARRAY(databaseSchema)),
                                DataTypes.FIELD("type", DataTypes.STRING()),
                                DataTypes.FIELD("database", DataTypes.STRING()),
                                DataTypes.FIELD("table", DataTypes.STRING()),
                                DataTypes.FIELD("pkNames", DataTypes.ARRAY(DataTypes.STRING())))
                        .getLogicalType();
    }
}
