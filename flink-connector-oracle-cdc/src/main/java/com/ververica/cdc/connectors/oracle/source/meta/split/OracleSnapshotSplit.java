/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.meta.split;

import org.apache.flink.table.types.logical.RowType;

import com.ververica.cdc.connectors.oracle.source.meta.offset.RedoLogOffset;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** The split to describe a split of an Oracle table snapshot. */
public class OracleSnapshotSplit extends OracleSplit {

    private final TableId tableId;
    private final RowType splitKeyType;
    private final Map<TableId, TableChanges.TableChange> tableSchemas;

    @Nullable private final Object[] splitStart;
    @Nullable private final Object[] splitEnd;
    /** The high watermark is not bull when the split read finished. */
    @Nullable private final RedoLogOffset highWatermark;

    @Nullable transient byte[] serializedFormCache;

    public OracleSnapshotSplit(
            String splitId,
            TableId tableId,
            RowType splitKeyType,
            Map<TableId, TableChanges.TableChange> tableSchemas,
            @Nullable Object[] splitStart,
            @Nullable Object[] splitEnd,
            @Nullable RedoLogOffset highWatermark) {
        super(splitId);
        this.tableId = tableId;
        this.splitKeyType = splitKeyType;
        this.tableSchemas = tableSchemas;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
    }

    @Override
    public Map<TableId, TableChanges.TableChange> getTableSchemas() {
        return tableSchemas;
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
    public RedoLogOffset getHighWatermark() {
        return highWatermark;
    }

    public boolean isSnapshotReadFinished() {
        return highWatermark != null;
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
        OracleSnapshotSplit that = (OracleSnapshotSplit) o;
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
        return "OracleSnapshotSplit{"
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
