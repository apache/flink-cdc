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

package org.apache.flink.cdc.common.event;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.data.RecordData;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Class {@code DataChangeEvent} represents the data change events of external systems, such as
 * INSERT, UPDATE, DELETE and so on.
 */
@PublicEvolving
public class DataChangeEvent implements ChangeEvent, Serializable {

    private static final long serialVersionUID = 1L;

    private DataChangeEvent(
            TableId tableId,
            RecordData before,
            RecordData after,
            OperationType op,
            Map<String, String> meta) {
        this.tableId = tableId;
        this.before = before;
        this.after = after;
        this.op = op;
        this.meta = meta;
    }

    private final TableId tableId;

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

    public RecordData before() {
        return before;
    }

    public RecordData after() {
        return after;
    }

    public OperationType op() {
        return op;
    }

    public String opTypeString(boolean isAfter) {
        switch (op) {
            case INSERT:
                return "+I";
            case UPDATE:
                return isAfter ? "+U" : "-U";
            case DELETE:
                return "-D";
            default:
                throw new UnsupportedOperationException("Unknown operation type: " + op);
        }
    }

    public Map<String, String> meta() {
        return meta;
    }

    /** Creates a {@link DataChangeEvent} instance that describes the insert event. */
    public static DataChangeEvent insertEvent(TableId tableId, RecordData after) {
        return new DataChangeEvent(
                tableId, null, after, OperationType.INSERT, Collections.emptyMap());
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the insert event with meta info.
     */
    public static DataChangeEvent insertEvent(
            TableId tableId, RecordData after, Map<String, String> meta) {
        return new DataChangeEvent(tableId, null, after, OperationType.INSERT, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the delete event. */
    public static DataChangeEvent deleteEvent(TableId tableId, RecordData before) {
        return new DataChangeEvent(
                tableId, before, null, OperationType.DELETE, Collections.emptyMap());
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the delete event with meta info.
     */
    public static DataChangeEvent deleteEvent(
            TableId tableId, RecordData before, Map<String, String> meta) {
        return new DataChangeEvent(tableId, before, null, OperationType.DELETE, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the update event. */
    public static DataChangeEvent updateEvent(
            TableId tableId, RecordData before, RecordData after) {
        return new DataChangeEvent(
                tableId, before, after, OperationType.UPDATE, Collections.emptyMap());
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the update event with meta info.
     */
    public static DataChangeEvent updateEvent(
            TableId tableId, RecordData before, RecordData after, Map<String, String> meta) {
        return new DataChangeEvent(tableId, before, after, OperationType.UPDATE, meta);
    }

    /** Creates a {@link DataChangeEvent} instance that describes the replace event. */
    public static DataChangeEvent replaceEvent(TableId tableId, RecordData after) {
        return new DataChangeEvent(
                tableId, null, after, OperationType.REPLACE, Collections.emptyMap());
    }

    /**
     * Creates a {@link DataChangeEvent} instance that describes the replace event with meta info.
     */
    public static DataChangeEvent replaceEvent(
            TableId tableId, RecordData after, Map<String, String> meta) {
        return new DataChangeEvent(tableId, null, after, OperationType.REPLACE, meta);
    }

    /**
     * Updates the before of a {@link DataChangeEvent} instance that describes the event with meta
     * info.
     */
    public static DataChangeEvent projectBefore(
            DataChangeEvent dataChangeEvent, RecordData projectedBefore) {
        return new DataChangeEvent(
                dataChangeEvent.tableId,
                projectedBefore,
                dataChangeEvent.after,
                dataChangeEvent.op,
                dataChangeEvent.meta);
    }

    /**
     * Updates the after of a {@link DataChangeEvent} instance that describes the event with meta
     * info.
     */
    public static DataChangeEvent projectAfter(
            DataChangeEvent dataChangeEvent, RecordData projectedAfter) {
        return new DataChangeEvent(
                dataChangeEvent.tableId,
                dataChangeEvent.before,
                projectedAfter,
                dataChangeEvent.op,
                dataChangeEvent.meta);
    }

    /**
     * Updates the after of a {@link DataChangeEvent} instance that describes the event with meta
     * info.
     */
    public static DataChangeEvent projectRecords(
            DataChangeEvent dataChangeEvent,
            RecordData projectedBefore,
            RecordData projectedAfter) {
        return new DataChangeEvent(
                dataChangeEvent.tableId,
                projectedBefore,
                projectedAfter,
                dataChangeEvent.op,
                dataChangeEvent.meta);
    }

    /** Updates the {@link TableId} info of current data change event. */
    public static DataChangeEvent route(DataChangeEvent dataChangeEvent, TableId tableId) {
        return new DataChangeEvent(
                tableId,
                dataChangeEvent.before,
                dataChangeEvent.after,
                dataChangeEvent.op,
                dataChangeEvent.meta);
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
                && Objects.equals(before, that.before)
                && Objects.equals(after, that.after)
                && op == that.op
                && Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, before, after, op, meta);
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

    public String toReadableString(Function<RecordData, ?> extractor) {
        return "DataChangeEvent{"
                + "tableId="
                + tableId
                + ", before="
                + extractor.apply(before)
                + ", after="
                + extractor.apply(after)
                + ", op="
                + op
                + ", meta="
                + describeMeta()
                + '}';
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
