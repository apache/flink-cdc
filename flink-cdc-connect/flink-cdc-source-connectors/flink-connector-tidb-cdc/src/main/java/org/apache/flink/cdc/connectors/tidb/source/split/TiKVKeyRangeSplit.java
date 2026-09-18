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

package org.apache.flink.cdc.connectors.tidb.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.cdc.common.annotation.Internal;

import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.Arrays;
import java.util.Objects;

/**
 * A contiguous key-range split of a TiDB table. The range is the reader's ownership of the table
 * key space and is not rebound to live TiKV region ids.
 */
@Internal
public class TiKVKeyRangeSplit implements SourceSplit {

    /** Sentinel resolvedTs meaning snapshot (if enabled) has not been completed. */
    public static final long NO_RESOLVED_TS = -1L;

    private final String splitId;
    private final byte[] startKey;
    private final byte[] endKey;
    private final long resolvedTs;

    public TiKVKeyRangeSplit(String splitId, byte[] startKey, byte[] endKey, long resolvedTs) {
        this.splitId = Objects.requireNonNull(splitId, "splitId");
        this.startKey = Objects.requireNonNull(startKey, "startKey");
        this.endKey = Objects.requireNonNull(endKey, "endKey");
        this.resolvedTs = resolvedTs;
    }

    public static TiKVKeyRangeSplit fromKeyRange(String splitId, KeyRange keyRange) {
        return fromKeyRange(splitId, keyRange, NO_RESOLVED_TS);
    }

    public static TiKVKeyRangeSplit fromKeyRange(
            String splitId, KeyRange keyRange, long resolvedTs) {
        return new TiKVKeyRangeSplit(
                splitId,
                keyRange.getStart().toByteArray(),
                keyRange.getEnd().toByteArray(),
                resolvedTs);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public long getResolvedTs() {
        return resolvedTs;
    }

    public KeyRange toKeyRange() {
        return KeyRangeUtils.makeCoprocRange(
                ByteString.copyFrom(startKey), ByteString.copyFrom(endKey));
    }

    public boolean isEmpty() {
        return Arrays.equals(startKey, endKey);
    }

    public boolean snapshotCompleted() {
        return resolvedTs != NO_RESOLVED_TS;
    }

    public TiKVKeyRangeSplit withResolvedTs(long newResolvedTs) {
        return new TiKVKeyRangeSplit(splitId, startKey, endKey, newResolvedTs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TiKVKeyRangeSplit that = (TiKVKeyRangeSplit) o;
        return resolvedTs == that.resolvedTs
                && splitId.equals(that.splitId)
                && Arrays.equals(startKey, that.startKey)
                && Arrays.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(splitId, resolvedTs);
        result = 31 * result + Arrays.hashCode(startKey);
        result = 31 * result + Arrays.hashCode(endKey);
        return result;
    }

    @Override
    public String toString() {
        return "TiKVKeyRangeSplit{id="
                + splitId
                + ", resolvedTs="
                + resolvedTs
                + ", empty="
                + isEmpty()
                + '}';
    }
}
