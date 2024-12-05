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

package org.apache.flink.cdc.connectors.base.source.meta.split;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The split to describe a split of a database table snapshot. */
public class SnapshotSplit extends SourceSplitBase {

    private final TableId tableId;
    private final RowType splitKeyType;
    private final Map<TableId, TableChange> tableSchemas;

    @Nullable private final Object[] splitStart;
    @Nullable private final Object[] splitEnd;
    /** The high watermark is not null when the split read finished. */
    @Nullable private final Offset highWatermark;

    @Nullable transient byte[] serializedFormCache;

    /**
     * Create a SnapshotSplit with generating splitId with the given tableId and chunkId.
     *
     * @see #generateSplitId(TableId, int)
     */
    public SnapshotSplit(
            TableId tableId,
            int chunkId,
            RowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            Offset highWatermark,
            Map<TableId, TableChange> tableSchemas) {
        super(generateSplitId(tableId, chunkId));
        this.tableId = tableId;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
        this.tableSchemas = tableSchemas;
    }

    /**
     * This constructor should not be used directly. Please use the other constructor. If this
     * constructor must be invoked, please use the same format for the splitId as {@link
     * #generateSplitId(TableId, int)}. Or else the parsing method will fail. See more in {@link
     * #extractTableId(String)} and {@link #extractChunkId(String)}.
     */
    @Internal
    public SnapshotSplit(
            TableId tableId,
            String splitId,
            RowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            Offset highWatermark,
            Map<TableId, TableChange> tableSchemas) {
        super(splitId);
        this.tableId = tableId;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
        this.tableSchemas = tableSchemas;
    }

    public TableId getTableId() {
        return tableId;
    }

    @Nullable
    public Object[] getSplitStart() {
        return splitStart;
    }

    @Nullable
    public Object[] getSplitEnd() {
        return splitEnd;
    }

    @Nullable
    public Offset getHighWatermark() {
        return highWatermark;
    }

    public boolean isSnapshotReadFinished() {
        return highWatermark != null;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return tableSchemas;
    }

    /** Casts this split into a {@link SchemalessSnapshotSplit}. */
    public final SchemalessSnapshotSplit toSchemalessSnapshotSplit() {
        return new SchemalessSnapshotSplit(
                tableId, splitId, splitKeyType, splitStart, splitEnd, highWatermark);
    }

    public static String generateSplitId(TableId tableId, int chunkId) {
        return tableId.toString() + ":" + chunkId;
    }

    public static TableId extractTableId(String splitId) {
        return TableId.parse(splitId.substring(0, splitId.lastIndexOf(":")));
    }

    public static int extractChunkId(String splitId) {
        return Integer.parseInt(splitId.substring(splitId.lastIndexOf(":") + 1));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SnapshotSplit that = (SnapshotSplit) o;
        return Objects.equals(tableId, that.tableId)
                && Objects.equals(splitKeyType, that.splitKeyType)
                && Arrays.equals(splitStart, that.splitStart)
                && Arrays.equals(splitEnd, that.splitEnd)
                && Objects.equals(highWatermark, that.highWatermark);
    }

    public RowType getSplitKeyType() {
        return splitKeyType;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), tableId, splitKeyType, highWatermark);
        result = 31 * result + Arrays.hashCode(splitStart);
        result = 31 * result + Arrays.hashCode(splitEnd);
        result = 31 * result + Arrays.hashCode(serializedFormCache);
        return result;
    }

    @Override
    public String toString() {
        String splitKeyTypeSummary =
                splitKeyType.getFields().stream()
                        .map(RowType.RowField::asSummaryString)
                        .collect(Collectors.joining(",", "[", "]"));
        return "SnapshotSplit{"
                + "tableId="
                + tableId
                + ", splitId='"
                + splitId
                + '\''
                + ", splitKeyType="
                + splitKeyTypeSummary
                + ", splitStart="
                + Arrays.toString(splitStart)
                + ", splitEnd="
                + Arrays.toString(splitEnd)
                + ", highWatermark="
                + highWatermark
                + '}';
    }
}
