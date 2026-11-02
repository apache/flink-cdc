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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/** Utils for convertion of {@link RowData} and {@link DataChangeEvent}. */
public class RowDataUtils {

    /**
     * Convert a {@link DataChangeEvent} to zero, one or two {@link RowData}.
     *
     * <p>For tables with equality fields, an {@code UPDATE} is split into a {@code DELETE} built
     * from the before-image followed by an {@code INSERT} built from the after-image. Iceberg
     * derives both the equality-delete key and the delete's target partition from the row passed to
     * the writer; collapsing an UPDATE to just the after-image (as before) means a partition key
     * change is never deleted from its old partition, leaving a stale duplicate behind.
     *
     * <p>Append-only writers do not interpret {@link RowKind#DELETE}. For tables without equality
     * fields, only the after-image of an update is appended, and deletes are ignored.
     */
    public static List<RowData> convertDataChangeEventToRowData(
            DataChangeEvent dataChangeEvent,
            List<RecordData.FieldGetter> fieldGetters,
            boolean supportsEqualityDeletes) {
        return convertDataChangeEventToRowData(
                dataChangeEvent, fieldGetters, supportsEqualityDeletes, null);
    }

    /**
     * Like {@link #convertDataChangeEventToRowData(DataChangeEvent, List, boolean)}, but the
     * before-image {@code DELETE} of an {@code UPDATE} is skipped when none of the columns read by
     * {@code deleteKeyGetters} changed. The upsert {@code INSERT} already deletes the after-image
     * key in the after-image partition, which is then exactly the row being replaced.
     *
     * <p>{@code deleteKeyGetters} must cover every equality field and every partition source column
     * of the target table: the upsert delete only covers the new key, and equality deletes are
     * partition-scoped. Pass {@code null} to always emit the {@code DELETE}.
     */
    public static List<RowData> convertDataChangeEventToRowData(
            DataChangeEvent dataChangeEvent,
            List<RecordData.FieldGetter> fieldGetters,
            boolean supportsEqualityDeletes,
            @Nullable RecordData.FieldGetter[] deleteKeyGetters) {
        switch (dataChangeEvent.op()) {
            case INSERT:
            case REPLACE:
                return Collections.singletonList(
                        toRowData(dataChangeEvent.after(), fieldGetters, RowKind.INSERT));
            case DELETE:
                if (!supportsEqualityDeletes) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(
                        toRowData(dataChangeEvent.before(), fieldGetters, RowKind.DELETE));
            case UPDATE:
                if (!supportsEqualityDeletes) {
                    return Collections.singletonList(
                            toRowData(dataChangeEvent.after(), fieldGetters, RowKind.INSERT));
                }
                if (deleteKeyGetters != null
                        && !anyFieldChanged(
                                dataChangeEvent.before(),
                                dataChangeEvent.after(),
                                deleteKeyGetters)) {
                    return Collections.singletonList(
                            toRowData(dataChangeEvent.after(), fieldGetters, RowKind.INSERT));
                }
                return Arrays.asList(
                        toRowData(dataChangeEvent.before(), fieldGetters, RowKind.DELETE),
                        toRowData(dataChangeEvent.after(), fieldGetters, RowKind.INSERT));
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
    }

    private static RowData toRowData(
            RecordData recordData, List<RecordData.FieldGetter> fieldGetters, RowKind kind) {
        GenericRowData genericRowData = new GenericRowData(recordData.getArity());
        genericRowData.setRowKind(kind);
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRowData;
    }

    /**
     * Creates getters for the given columns that return CDC internal values, so that comparing them
     * avoids the conversions done by the Iceberg field getters. Returns {@code null} if a column is
     * not in {@code schema}, e.g. a nested identifier field.
     */
    @Nullable
    public static RecordData.FieldGetter[] createDeleteKeyGetters(
            Schema schema, Collection<String> columnNames) {
        List<String> schemaColumns = schema.getColumnNames();
        List<DataType> types = schema.getColumnDataTypes();
        List<RecordData.FieldGetter> getters = new ArrayList<>();
        for (String name : new LinkedHashSet<>(columnNames)) {
            int pos = schemaColumns.indexOf(name);
            if (pos < 0) {
                return null;
            }
            getters.add(RecordData.createFieldGetter(types.get(pos), pos));
        }
        return getters.toArray(new RecordData.FieldGetter[0]);
    }

    private static boolean anyFieldChanged(
            RecordData before, RecordData after, RecordData.FieldGetter[] getters) {
        for (RecordData.FieldGetter getter : getters) {
            if (!Objects.deepEquals(getter.getFieldOrNull(before), getter.getFieldOrNull(after))) {
                return true;
            }
        }
        return false;
    }
}
