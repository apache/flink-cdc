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

package org.apache.flink.cdc.connectors.kafka.format.csv;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.Configuration;
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
import org.apache.flink.formats.csv.CsvRowDataSerializationSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.commons.text.StringEscapeUtils;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.QUOTE_CHARACTER;
import static org.apache.flink.cdc.connectors.kafka.format.csv.CsvFormatOptions.WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION;

/** A {@link SerializationSchema} to convert {@link Event} into byte of csv format. */
public class CsvSerializationSchema implements SerializationSchema<Event> {

    private static final long serialVersionUID = 1L;

    /**
     * A map of {@link TableId} and its {@link SerializationSchema} to serialize Debezium JSON data.
     */
    private final Map<TableId, TableSchemaInfo> csvSerializers;

    private final ZoneId zoneId;

    private InitializationContext context;

    private Configuration formatOptions;

    public CsvSerializationSchema(ZoneId zoneId, Configuration formatOptions) {
        this.zoneId = zoneId;
        this.formatOptions = formatOptions;
        csvSerializers = new HashMap<>();
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
                                csvSerializers.get(schemaChangeEvent.tableId()).getSchema(),
                                schemaChangeEvent);
            }
            CsvRowDataSerializationSchema csvSerializer = buildSerializationForPrimaryKey(schema);
            try {
                csvSerializer.open(context);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            csvSerializers.put(
                    schemaChangeEvent.tableId(),
                    new TableSchemaInfo(
                            schemaChangeEvent.tableId(), schema, csvSerializer, zoneId));
            return null;
        }
        DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
        RecordData recordData =
                dataChangeEvent.op().equals(OperationType.DELETE)
                        ? dataChangeEvent.before()
                        : dataChangeEvent.after();
        TableSchemaInfo tableSchemaInfo = csvSerializers.get(dataChangeEvent.tableId());
        return tableSchemaInfo
                .getSerializationSchema()
                .serialize(tableSchemaInfo.getRowDataFromRecordData(recordData, true));
    }

    private CsvRowDataSerializationSchema buildSerializationForPrimaryKey(Schema schema) {
        DataField[] fields = new DataField[schema.primaryKeys().size() + 1];
        fields[0] = DataTypes.FIELD("TableId", DataTypes.STRING());
        for (int i = 0; i < schema.primaryKeys().size(); i++) {
            Column column = schema.getColumn(schema.primaryKeys().get(i)).get();
            fields[i + 1] = DataTypes.FIELD(column.getName(), column.getType());
        }
        // the row should never be null
        DataType dataType = DataTypes.ROW(fields).notNull();
        LogicalType rowType = DataTypeUtils.toFlinkDataType(dataType).getLogicalType();
        CsvRowDataSerializationSchema.Builder schemaBuilder =
                new CsvRowDataSerializationSchema.Builder((RowType) rowType);
        configureSerializationSchema(formatOptions, schemaBuilder);
        return schemaBuilder.build();
    }

    private static void configureSerializationSchema(
            Configuration formatOptions, CsvRowDataSerializationSchema.Builder schemaBuilder) {
        formatOptions
                .getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(schemaBuilder::setFieldDelimiter);

        if (formatOptions.get(DISABLE_QUOTE_CHARACTER)) {
            schemaBuilder.disableQuoteCharacter();
        } else {
            formatOptions
                    .getOptional(QUOTE_CHARACTER)
                    .map(quote -> quote.charAt(0))
                    .ifPresent(schemaBuilder::setQuoteCharacter);
        }

        formatOptions
                .getOptional(ARRAY_ELEMENT_DELIMITER)
                .ifPresent(schemaBuilder::setArrayElementDelimiter);

        formatOptions
                .getOptional(ESCAPE_CHARACTER)
                .map(escape -> escape.charAt(0))
                .ifPresent(schemaBuilder::setEscapeCharacter);

        formatOptions.getOptional(NULL_LITERAL).ifPresent(schemaBuilder::setNullLiteral);

        formatOptions
                .getOptional(WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION)
                .ifPresent(schemaBuilder::setWriteBigDecimalInScientificNotation);
    }
}
