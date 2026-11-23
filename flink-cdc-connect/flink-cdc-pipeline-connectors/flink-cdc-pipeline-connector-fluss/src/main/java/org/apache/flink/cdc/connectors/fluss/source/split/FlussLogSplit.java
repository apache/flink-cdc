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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * A split that reads change log records from a Fluss table bucket starting at a given offset. This
 * is used for streaming CDC reads (earliest, latest, timestamp startup modes).
 */
public class FlussLogSplit extends FlussSplitBase {

    private final long startingOffset;

    public FlussLogSplit(
            PhysicalTablePath tablePath, TableBucket tableBucket, long startingOffset) {
        this(tablePath, tableBucket, startingOffset, null, null);
    }

    /** Full constructor, typically used during checkpoint recovery. */
    public FlussLogSplit(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            long startingOffset,
            @Nullable Integer schemaId,
            @Nullable RowType rowType) {
        super(tablePath, tableBucket, schemaId, rowType);
        this.startingOffset = startingOffset;
    }

    public long getStartingOffset() {
        return startingOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FlussLogSplit that = (FlussLogSplit) o;
        return startingOffset == that.startingOffset
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(tableBucket, that.tableBucket);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tablePath, tableBucket, startingOffset);
    }

    @Override
    public String toString() {
        return "FlussLogSplit{"
                + "tablePath="
                + tablePath
                + ", tableBucket="
                + tableBucket
                + ", startingOffset="
                + startingOffset
                + '}';
    }
}
