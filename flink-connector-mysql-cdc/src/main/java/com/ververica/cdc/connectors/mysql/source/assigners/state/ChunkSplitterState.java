/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.assigners.state;

import com.ververica.cdc.connectors.mysql.source.assigners.MySqlChunkSplitter;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Objects;

/** The state of the {@link MySqlChunkSplitter}. */
public class ChunkSplitterState {

    public static final ChunkSplitterState NO_SPLITTING_TABLE_STATE =
            new ChunkSplitterState(null, null, null);

    /** Record current splitting table id in the chunk splitter. */
    @Nullable private final TableId currentSplittingTableId;

    /** Record next chunk start. */
    @Nullable private final ChunkBound nextChunkStart;

    /** Record next chunk id. */
    @Nullable private final Integer nextChunkId;

    public ChunkSplitterState(
            @Nullable TableId currentSplittingTableId,
            @Nullable ChunkBound nextChunkStart,
            @Nullable Integer nextChunkId) {
        this.currentSplittingTableId = currentSplittingTableId;
        this.nextChunkStart = nextChunkStart;
        this.nextChunkId = nextChunkId;
    }

    @Nullable
    public TableId getCurrentSplittingTableId() {
        return currentSplittingTableId;
    }

    @Nullable
    public ChunkBound getNextChunkStart() {
        return nextChunkStart;
    }

    @Nullable
    public Integer getNextChunkId() {
        return nextChunkId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChunkSplitterState)) {
            return false;
        }
        ChunkSplitterState that = (ChunkSplitterState) o;
        return Objects.equals(currentSplittingTableId, that.currentSplittingTableId)
                && Objects.equals(nextChunkStart, that.nextChunkStart)
                && Objects.equals(nextChunkId, that.nextChunkId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentSplittingTableId, nextChunkStart, nextChunkId);
    }

    @Override
    public String toString() {
        return "ChunkSplitterState{"
                + "currentSplittingTableId="
                + (currentSplittingTableId == null ? "null" : currentSplittingTableId)
                + ", nextChunkStart="
                + (nextChunkStart == null ? "null" : nextChunkStart)
                + ", nextChunkId="
                + (nextChunkId == null ? "null" : String.valueOf(nextChunkId))
                + '}';
    }

    /** The definition of the chunk bound. */
    public static final class ChunkBound {

        public static final ChunkBound START_BOUND = new ChunkBound(ChunkBoundType.START, null);
        public static final ChunkBound END_BOUND = new ChunkBound(ChunkBoundType.END, null);

        private final ChunkBoundType boundType;
        @Nullable private final Object value;

        public ChunkBound(ChunkBoundType boundType, @Nullable Object value) {
            this.boundType = boundType;
            this.value = value;
        }

        public ChunkBoundType getBoundType() {
            return boundType;
        }

        @Nullable
        public Object getValue() {
            return value;
        }

        public static ChunkBound middleOf(Object obj) {
            return new ChunkBound(ChunkBoundType.MIDDLE, obj);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ChunkBound)) {
                return false;
            }
            ChunkBound that = (ChunkBound) o;
            return boundType == that.boundType && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(boundType, value);
        }

        @Override
        public String toString() {
            return "ChunkBound{"
                    + "boundType="
                    + boundType
                    + ", value="
                    + (value == null ? "null" : value.toString())
                    + '}';
        }
    }

    /** The type of the chunk bound. */
    public enum ChunkBoundType {
        START,
        MIDDLE,
        END
    }
}
