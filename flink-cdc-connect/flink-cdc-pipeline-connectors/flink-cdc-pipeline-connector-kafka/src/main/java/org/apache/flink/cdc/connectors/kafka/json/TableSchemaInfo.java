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

package org.apache.flink.cdc.connectors.kafka.json;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.kafka.json.utils.RecordDataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** maintain the {@link SerializationSchema} of a specific {@link TableId}. */
public class TableSchemaInfo {

    private final TableId tableId;

    private final Schema schema;

    private final List<Integer> primaryKeyColumnIndexes;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final SerializationSchema<RowData> serializationSchema;

    public TableSchemaInfo(
            TableId tableId,
            Schema schema,
            SerializationSchema<RowData> serializationSchema,
            ZoneId zoneId) {
        this.tableId = tableId;
        this.schema = schema;
        this.serializationSchema = serializationSchema;
        this.fieldGetters = createFieldGetters(schema, zoneId);
        primaryKeyColumnIndexes = new ArrayList<>();
        for (int keyIndex = 0; keyIndex < schema.primaryKeys().size(); keyIndex++) {
            for (int columnIndex = 0; columnIndex < schema.getColumnCount(); columnIndex++) {
                if (schema.getColumns()
                        .get(columnIndex)
                        .getName()
                        .equals(schema.primaryKeys().get(keyIndex))) {
                    primaryKeyColumnIndexes.add(columnIndex);
                    break;
                }
            }
        }
    }

    /** convert to {@link RowData}, which will be pass to serializationSchema. */
    public RowData getRowDataFromRecordData(RecordData recordData, boolean primaryKeyOnly) {
        if (primaryKeyOnly) {
            GenericRowData genericRowData = new GenericRowData(primaryKeyColumnIndexes.size() + 1);
            genericRowData.setField(0, StringData.fromString(tableId.toString()));
            for (int i = 0; i < primaryKeyColumnIndexes.size(); i++) {
                genericRowData.setField(
                        i + 1,
                        fieldGetters
                                .get(primaryKeyColumnIndexes.get(i))
                                .getFieldOrNull(recordData));
            }
            return genericRowData;
        } else {
            GenericRowData genericRowData = new GenericRowData(recordData.getArity());
            for (int i = 0; i < recordData.getArity(); i++) {
                genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
            }
            return genericRowData;
        }
    }

    private static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        return RecordDataConverter.createFieldGetters(schema, zoneId);
    }

    public Schema getSchema() {
        return schema;
    }

    public SerializationSchema<RowData> getSerializationSchema() {
        return serializationSchema;
    }
}
