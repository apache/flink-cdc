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

package com.ververica.cdc.common.event;

import com.ververica.cdc.common.annotation.PublicEvolving;
import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.types.RowType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * Class {@code DataChangeEvent} represents the data change events of external systems, such as
 * INSERT, UPDATE, DELETE and so on.
 */
@PublicEvolving
public class DataChangeEvent implements ChangeEvent, Serializable {

    private static final long serialVersionUID = 1L;

    private DataChangeEvent(
            TableId tableId,
            RowType recordDataType,
            RecordData before,
            RecordData after,
            OperationType op,
            Map<String, String> meta) {
        this.tableId = tableId;
        this.recordDataType = recordDataType;
        this.before = before;
        this.after = after;
        this.op = op;
        this.meta = meta;
    }

    private final TableId tableId;

    /**
     * The data type of record, the before record and after record always keep the same data type.
     */
    private final RowType recordDataType;

    /** Describes the record of data before change. */
    private final RecordData before;

    /** Describes the record of data after change. */
    private final RecordData after;

    /** Describes the operation type of the change event. e.g. INSERT, UPDATE, REPLACE, DELETE. */
    private final OperationType op;

    /** Optional, describes the metadata of the change event. e.g. MySQL binlog file name, pos. */
    private final Map<String, String> meta;

    @Override
    public TableId tableId() {
        return tableId;
    }

    public RowType getRecordDataType() {
        return recordDataType;
    }

    public RecordData before() {
        return before;
    }

    public RecordData after() {
        return after;
    }

    public OperationType op() {
        return op;
    }

    public Map<String, String> meta() {
        return meta;
    }

    /** Creates a {@link DataChangeEvent} instance that describes the insert event. */
    public static DataChangeEvent insertEvent(
            TableId tableId, RowType recordDataType, RecordData after) {
        return new DataChangeEvent(
                tableId, recordDataType, null, after, OperationType.INSERT, null);
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the insert event with meta info.
     */
    public static DataChangeEvent insertEvent(
            TableId tableId, RowType recordDataType, RecordData after, Map<String, String> meta) {
        return new DataChangeEvent(
                tableId, recordDataType, null, after, OperationType.INSERT, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the delete event. */
    public static DataChangeEvent deleteEvent(
            TableId tableId, RowType recordDataType, RecordData before) {
        return new DataChangeEvent(
                tableId, recordDataType, before, null, OperationType.DELETE, null);
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the delete event with meta info.
     */
    public static DataChangeEvent deleteEvent(
            TableId tableId, RowType recordDataType, RecordData before, Map<String, String> meta) {
        return new DataChangeEvent(
                tableId, recordDataType, before, null, OperationType.DELETE, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the update event. */
    public static DataChangeEvent updateEvent(
            TableId tableId, RowType recordDataType, RecordData before, RecordData after) {
        return new DataChangeEvent(
                tableId, recordDataType, before, after, OperationType.UPDATE, null);
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the update event with meta info.
     */
    public static DataChangeEvent updateEvent(
            TableId tableId,
            RowType recordDataType,
            RecordData before,
            RecordData after,
            Map<String, String> meta) {
        return new DataChangeEvent(
                tableId, recordDataType, before, after, OperationType.UPDATE, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the replace event. */
    public static DataChangeEvent replaceEvent(
            TableId tableId, RowType recordDataType, RecordData after) {
        return new DataChangeEvent(
                tableId, recordDataType, null, after, OperationType.REPLACE, null);
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the replace event with meta info.
     */
    public static DataChangeEvent replaceEvent(
            TableId tableId, RowType recordDataType, RecordData after, Map<String, String> meta) {
        return new DataChangeEvent(
                tableId, recordDataType, null, after, OperationType.REPLACE, meta);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataChangeEvent)) {
            return false;
        }
        DataChangeEvent that = (DataChangeEvent) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(recordDataType, that.recordDataType)
                && Objects.equals(before, that.before)
                && Objects.equals(after, that.after)
                && op == that.op
                && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, recordDataType, before, after, op, meta);
    }

    /** Creates a string to describe the information of meta. */
    public String describeMeta() {
        StringBuilder stringBuilder = new StringBuilder("(");
        if (meta != null && !meta.isEmpty()) {
            stringBuilder.append(meta);
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return "DataChangeEvent{"
                + "tableId="
                + tableId
                + ", before="
                + before
                + ", after="
                + after
                + ", op="
                + op
                + ", meta="
                + describeMeta()
                + '}';
    }
}
