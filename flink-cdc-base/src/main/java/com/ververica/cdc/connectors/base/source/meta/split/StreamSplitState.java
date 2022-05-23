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

import java.util.Map;

/** The state of split to describe the transaction of table(s). */
public class StreamSplitState<ID extends DataCollectionId, S> extends SourceSplitState<ID, S> {

    @Nullable private Offset startingOffset;
    @Nullable private Offset endingOffset;
    private final Map<ID, S> tableSchemas;

    public StreamSplitState(StreamSplit<ID, S> split) {
        super(split);
        this.startingOffset = split.getStartingOffset();
        this.endingOffset = split.getEndingOffset();
        this.tableSchemas = split.getTableSchemas();
    }

    @Nullable
    public Offset getStartingOffset() {
        return startingOffset;
    }

    public void setStartingOffset(@Nullable Offset startingOffset) {
        this.startingOffset = startingOffset;
    }

    @Nullable
    public Offset getEndingOffset() {
        return endingOffset;
    }

    public void setEndingOffset(@Nullable Offset endingOffset) {
        this.endingOffset = endingOffset;
    }

    public Map<ID, S> getTableSchemas() {
        return tableSchemas;
    }

    public void recordSchema(ID tableId, S latestTableChange) {
        this.tableSchemas.put(tableId, latestTableChange);
    }

    @Override
    public StreamSplit<ID, S> toSourceSplit() {
        final StreamSplit<ID, S> streamSplit = split.asStreamSplit();
        return new StreamSplit<>(
                streamSplit.splitId(),
                getStartingOffset(),
                getEndingOffset(),
                streamSplit.asStreamSplit().getFinishedSnapshotSplitInfos(),
                getTableSchemas(),
                streamSplit.getTotalFinishedSplitSize());
    }

    @Override
    public String toString() {
        return "StreamSplitState{"
                + "startingOffset="
                + startingOffset
                + ", endingOffset="
                + endingOffset
                + ", split="
                + split
                + '}';
    }
}
