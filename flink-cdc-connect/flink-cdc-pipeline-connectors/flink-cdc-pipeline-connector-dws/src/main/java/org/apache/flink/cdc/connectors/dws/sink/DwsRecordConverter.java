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

package org.apache.flink.cdc.connectors.dws.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.connectors.dws.utils.DwsUtils;

import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts Flink CDC internal rows into values accepted by the native DWS client. */
public final class DwsRecordConverter {

    private final Schema schema;
    private final RecordData.FieldGetter[] fieldGetters;
    private final int[] primaryKeyIndexes;

    public DwsRecordConverter(Schema schema, ZoneId zoneId) {
        this.schema = Preconditions.checkNotNull(schema, "Schema must not be null.");
        Preconditions.checkNotNull(zoneId, "Zone ID must not be null.");

        List<Column> columns = schema.getColumns();
        this.fieldGetters = new RecordData.FieldGetter[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters[i] = DwsUtils.createFieldGetter(columns.get(i).getType(), i, zoneId);
        }

        List<String> primaryKeys = schema.primaryKeys();
        Preconditions.checkArgument(
                !primaryKeys.isEmpty(), "DWS CDC writes require a primary key.");
        this.primaryKeyIndexes = new int[primaryKeys.size()];
        for (int i = 0; i < primaryKeys.size(); i++) {
            primaryKeyIndexes[i] = findColumnIndex(columns, primaryKeys.get(i));
        }
    }

    /** Converts all columns for INSERT, UPDATE, or REPLACE. */
    public Map<String, Object> convertWrite(RecordData record) {
        validateRecord(record);
        Map<String, Object> values = new LinkedHashMap<>(fieldGetters.length);
        List<Column> columns = schema.getColumns();
        for (int i = 0; i < fieldGetters.length; i++) {
            values.put(columns.get(i).getName(), fieldGetters[i].getFieldOrNull(record));
        }
        return values;
    }

    /** Converts only primary-key columns for DELETE or an update-before retraction. */
    public Map<String, Object> convertDelete(RecordData record) {
        validateRecord(record);
        Map<String, Object> values = new LinkedHashMap<>(primaryKeyIndexes.length);
        List<Column> columns = schema.getColumns();
        for (int primaryKeyIndex : primaryKeyIndexes) {
            values.put(
                    columns.get(primaryKeyIndex).getName(),
                    fieldGetters[primaryKeyIndex].getFieldOrNull(record));
        }
        return values;
    }

    private void validateRecord(RecordData record) {
        Preconditions.checkNotNull(record, "Record must not be null.");
        Preconditions.checkArgument(
                record.getArity() == fieldGetters.length,
                "Record arity %s does not match schema column count %s.",
                record.getArity(),
                fieldGetters.length);
    }

    private static int findColumnIndex(List<Column> columns, String columnName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException(
                String.format("Primary key %s is missing from the DWS schema.", columnName));
    }
}
