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

package com.ververica.cdc.connectors.mysql.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * An implementation of {@link RecordsWithSplitIds} which contains the records of one table split.
 */
public final class MySqlRecords implements RecordsWithSplitIds<SourceRecord> {

    @Nullable private String splitId;
    @Nullable private Iterator<SourceRecord> recordsForCurrentSplit;
    @Nullable private final Iterator<SourceRecord> recordsForSplit;
    private final Set<String> finishedSnapshotSplits;

    public MySqlRecords(
            @Nullable String splitId,
            @Nullable Iterator recordsForSplit,
            Set<String> finishedSnapshotSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForCurrentSplit = nextSplit != null ? this.recordsForSplit : null;
        return nextSplit;
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        final Iterator<SourceRecord> recordsForSplit = this.recordsForCurrentSplit;
        if (recordsForSplit != null) {
            if (recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSnapshotSplits;
    }

    public static MySqlRecords forRecords(
            final String splitId, final Iterator<SourceRecord> recordsForSplit) {
        return new MySqlRecords(splitId, recordsForSplit, Collections.emptySet());
    }

    public static MySqlRecords forFinishedSplit(final String splitId) {
        return new MySqlRecords(null, null, Collections.singleton(splitId));
    }
}
