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

package org.apache.flink.cdc.connectors.postgres.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.time.Conversions;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The offset for Postgres. */
public class PostgresOffset extends Offset {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(PostgresOffset.class);

    public static final PostgresOffset INITIAL_OFFSET =
            new PostgresOffset(Lsn.INVALID_LSN.asLong(), null, Instant.MIN);
    public static final PostgresOffset NO_STOPPING_OFFSET =
            new PostgresOffset(Lsn.NO_STOPPING_LSN.asLong(), null, Instant.MAX);

    // used by PostgresOffsetFactory
    PostgresOffset(Map<String, String> offset) {
        Map<String, String> filtered = new HashMap<>(offset);
        // When a checkpoint is taken right after a COMMIT (state-3a), all three LSN fields
        // converge to the same value: lsn == lsn_proc == lsn_commit.
        // Recovering from such a checkpoint constructs WalPositionLocator(C0, C0), which
        // causes the first new transaction's DML records (data_start=C0 in pgoutput) to be
        // silently dropped: they are added to lsnSeen during the find phase, but
        // startStreamingLsn is set to the next COMMIT (C1), so they are filtered in the
        // stream phase.
        //
        // Fix: when lsn == lsn_proc == lsn_commit, remove lsn_proc and lsn_commit so that
        // WalPositionLocator is constructed with lastCommitStoredLsn=null, which triggers
        // the fast path: startStreamingLsn=firstLsnReceived=C0, all messages pass through.
        //
        // The triple-equality condition is safe: mid-transaction checkpoints (state-3b) have
        // lsn_commit pointing to the previous commit, so lsn_commit != lsn, and this branch
        // is not taken.
        //
        // This workaround can be removed once Debezium is upgraded to a version that
        // includes DBZ-6204:
        // https://github.com/debezium/debezium/commit/3b5740f1a836c8b438888f2458ebb1554320bac7
        String lsnVal = filtered.get(SourceInfo.LSN_KEY);
        String lsnProc = filtered.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        String lsnCommit = filtered.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        if (lsnVal != null && lsnVal.equals(lsnProc) && lsnVal.equals(lsnCommit)) {
            filtered.remove(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
            filtered.remove(PostgresOffsetContext.LAST_COMMIT_LSN_KEY);
        }
        this.offset = filtered;
    }

    PostgresOffset(Long lsn, Long txId, Instant lastCommitTs) {
        Map<String, String> offsetMap = new HashMap<>();
        // keys are from io.debezium.connector.postgresql.PostgresOffsetContext.Loader.load
        offsetMap.put(SourceInfo.LSN_KEY, lsn.toString());
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, txId.toString());
        }
        if (lastCommitTs != null) {
            offsetMap.put(
                    SourceInfo.TIMESTAMP_USEC_KEY,
                    String.valueOf(Conversions.toEpochMicros(lastCommitTs)));
        }
        this.offset = offsetMap;
    }

    public static PostgresOffset of(SourceRecord dataRecord) {
        return of(dataRecord.sourceOffset());
    }

    public static PostgresOffset of(Map<String, ?> offsetMap) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }

        return new PostgresOffset(offsetStrMap);
    }

    public Lsn getLsn() {
        return Lsn.valueOf(Long.valueOf(this.offset.get(SourceInfo.LSN_KEY)));
    }

    @Nullable
    public Long getTxid() {
        String txid = this.offset.get(SourceInfo.TXID_KEY);
        return txid == null ? null : Long.valueOf(txid);
    }

    @Nullable
    public Long getLastCommitTs() {
        String lastCommitTs = this.offset.get(SourceInfo.TIMESTAMP_USEC_KEY);
        return lastCommitTs == null ? null : Long.valueOf(lastCommitTs);
    }

    @Override
    public int compareTo(Offset o) {
        PostgresOffset rhs = (PostgresOffset) o;
        LOG.debug("comparing {} and {}", this, rhs);
        return this.getLsn().compareTo(rhs.getLsn());
    }

    @Override
    public String toString() {
        return "Offset{lsn="
                + getLsn()
                + ", txId="
                + (getTxid() == null ? "null" : getTxid())
                + ", lastCommitTs="
                + (getLastCommitTs() == null ? "null" : getLastCommitTs())
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PostgresOffset)) {
            return false;
        }
        PostgresOffset that = (PostgresOffset) o;
        return offset.equals(that.offset);
    }
}
