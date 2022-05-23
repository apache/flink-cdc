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

package com.ververica.cdc.connectors.base.source.meta.split;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.schema.DataCollectionId;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** The split to describe the transaction log split. */
public class StreamSplit<ID extends DataCollectionId, S> extends SourceSplitBase<ID, S> {

    public static final String STREAM_SPLIT_ID = "stream-split";

    private final Offset startingOffset;
    private final Offset endingOffset;
    private final List<FinishedSnapshotSplitInfo<ID>> finishedSnapshotSplitInfos;
    private final Map<ID, S> tableSchemas;
    private final int totalFinishedSplitSize;
    @Nullable transient byte[] serializedFormCache;

    public StreamSplit(
            String splitId,
            Offset startingOffset,
            Offset endingOffset,
            List<FinishedSnapshotSplitInfo<ID>> finishedSnapshotSplitInfos,
            Map<ID, S> tableSchemas,
            int totalFinishedSplitSize) {
        super(splitId);
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        this.finishedSnapshotSplitInfos = finishedSnapshotSplitInfos;
        this.tableSchemas = tableSchemas;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
    }

    public Offset getStartingOffset() {
        return startingOffset;
    }

    public Offset getEndingOffset() {
        return endingOffset;
    }

    public List<FinishedSnapshotSplitInfo<ID>> getFinishedSnapshotSplitInfos() {
        return finishedSnapshotSplitInfos;
    }

    @Override
    public Map<ID, S> getTableSchemas() {
        return tableSchemas;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }

    public boolean isCompletedSplit() {
        return totalFinishedSplitSize == finishedSnapshotSplitInfos.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StreamSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        StreamSplit<?, ?> that = (StreamSplit<?, ?>) o;
        return totalFinishedSplitSize == that.totalFinishedSplitSize
                && Objects.equals(startingOffset, that.startingOffset)
                && Objects.equals(endingOffset, that.endingOffset)
                && Objects.equals(finishedSnapshotSplitInfos, that.finishedSnapshotSplitInfos)
                && Objects.equals(tableSchemas, that.tableSchemas);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                startingOffset,
                endingOffset,
                finishedSnapshotSplitInfos,
                tableSchemas,
                totalFinishedSplitSize);
    }

    @Override
    public String toString() {
        return "StreamSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + startingOffset
                + ", endOffset="
                + endingOffset
                + '}';
    }

    // -------------------------------------------------------------------
    // factory utils to build new StreamSplit instance
    // -------------------------------------------------------------------
    public static <ID extends DataCollectionId, S> StreamSplit<ID, S> appendFinishedSplitInfos(
            StreamSplit<ID, S> streamSplit, List<FinishedSnapshotSplitInfo<ID>> splitInfos) {
        splitInfos.addAll(streamSplit.getFinishedSnapshotSplitInfos());
        return new StreamSplit<>(
                streamSplit.splitId,
                streamSplit.getStartingOffset(),
                streamSplit.getEndingOffset(),
                splitInfos,
                streamSplit.getTableSchemas(),
                streamSplit.getTotalFinishedSplitSize());
    }

    public static <ID extends DataCollectionId, S> StreamSplit<ID, S> fillTableSchemas(
            StreamSplit<ID, S> streamSplit, Map<ID, S> tableSchemas) {
        tableSchemas.putAll(streamSplit.getTableSchemas());
        return new StreamSplit<>(
                streamSplit.splitId,
                streamSplit.getStartingOffset(),
                streamSplit.getEndingOffset(),
                streamSplit.getFinishedSnapshotSplitInfos(),
                tableSchemas,
                streamSplit.getTotalFinishedSplitSize());
    }
}
