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

package org.apache.flink.cdc.connectors.dws.utils;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.StringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Writer-independent reducer used as the correctness oracle for DWS integration tests. */
public final class DwsExpectedState {

    private final List<RecordData.FieldGetter> rowGetters;
    private final List<RecordData.FieldGetter> primaryKeyGetters;
    private final Map<Row, Row> rowsByPrimaryKey = new LinkedHashMap<>();

    private DwsExpectedState(
            List<RecordData.FieldGetter> rowGetters,
            List<RecordData.FieldGetter> primaryKeyGetters) {
        this.rowGetters = rowGetters;
        this.primaryKeyGetters = primaryKeyGetters;
    }

    public static DwsExpectedState forSchema(Schema schema) {
        if (schema.primaryKeys().isEmpty()) {
            throw new IllegalArgumentException("DWS expected-state oracle requires a primary key");
        }

        List<RecordData.FieldGetter> rowGetters = new ArrayList<>();
        List<RecordData.FieldGetter> primaryKeyGetters = new ArrayList<>();
        for (int i = 0; i < schema.getColumnCount(); i++) {
            RecordData.FieldGetter getter =
                    RecordData.createFieldGetter(schema.getColumnDataTypes().get(i), i);
            rowGetters.add(getter);
            if (schema.primaryKeys().contains(schema.getColumnNames().get(i))) {
                primaryKeyGetters.add(getter);
            }
        }
        if (primaryKeyGetters.size() != schema.primaryKeys().size()) {
            throw new IllegalArgumentException("Primary key columns must exist in the schema");
        }
        return new DwsExpectedState(rowGetters, primaryKeyGetters);
    }

    public void apply(DataChangeEvent event) {
        OperationType operation = event.op();
        if (operation == OperationType.DELETE) {
            rowsByPrimaryKey.remove(extract(event.before(), primaryKeyGetters));
            return;
        }
        if (operation == OperationType.UPDATE_BEFORE) {
            rowsByPrimaryKey.remove(extract(event.before(), primaryKeyGetters));
            return;
        }
        if (operation == OperationType.UPDATE) {
            Row oldKey = extract(event.before(), primaryKeyGetters);
            Row newKey = extract(event.after(), primaryKeyGetters);
            if (!oldKey.equals(newKey)) {
                rowsByPrimaryKey.remove(oldKey);
            }
            rowsByPrimaryKey.put(newKey, extract(event.after(), rowGetters));
            return;
        }
        if (operation == OperationType.INSERT || operation == OperationType.REPLACE) {
            rowsByPrimaryKey.put(
                    extract(event.after(), primaryKeyGetters), extract(event.after(), rowGetters));
            return;
        }
        throw new IllegalArgumentException("Unsupported operation in DWS oracle: " + operation);
    }

    public void applyAll(Iterable<DataChangeEvent> events) {
        for (DataChangeEvent event : events) {
            apply(event);
        }
    }

    public Collection<Row> rows() {
        return Collections.unmodifiableCollection(new ArrayList<>(rowsByPrimaryKey.values()));
    }

    private static Row extract(RecordData record, List<RecordData.FieldGetter> getters) {
        if (record == null) {
            throw new IllegalArgumentException("Required record is missing from data change event");
        }
        Object[] values = new Object[getters.size()];
        for (int i = 0; i < getters.size(); i++) {
            values[i] = normalize(getters.get(i).getFieldOrNull(record));
        }
        return new Row(values);
    }

    private static Object normalize(Object value) {
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return Arrays.copyOf(bytes, bytes.length);
        }
        if (value instanceof StringData) {
            return value.toString();
        }
        if (value instanceof Object[]) {
            Object[] source = (Object[]) value;
            Object[] copy = new Object[source.length];
            for (int i = 0; i < source.length; i++) {
                copy[i] = normalize(source[i]);
            }
            return copy;
        }
        return value;
    }

    /** Deep-value row representation suitable for deterministic assertions. */
    public static final class Row {

        private final Object[] values;

        private Row(Object[] values) {
            this.values = values;
        }

        public static Row of(Object... values) {
            Object[] normalized = new Object[values.length];
            for (int i = 0; i < values.length; i++) {
                normalized[i] = normalize(values[i]);
            }
            return new Row(normalized);
        }

        public List<Object> values() {
            return Collections.unmodifiableList(
                    Arrays.asList(Arrays.copyOf(values, values.length)));
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Row && Arrays.deepEquals(values, ((Row) other).values);
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(values);
        }

        @Override
        public String toString() {
            return Arrays.deepToString(values);
        }
    }
}
