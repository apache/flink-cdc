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

package com.ververica.cdc.connectors.postgres.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.time.Conversions;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** The offset for Postgres. */
public class PostgresOffset extends Offset {

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(PostgresOffset.class);

    // ref: io.debezium.connector.postgresql.spi.OffsetState (which is not serializable)

    // workaround to make LSN serializable
    private final Long lsn;
    private final Long txid;
    private final Instant lastCommitTs;

    public static final PostgresOffset INITIAL_OFFSET =
            new PostgresOffset(Lsn.INVALID_LSN.asLong(), null, Instant.MIN);
    public static final PostgresOffset NO_STOPPING_OFFSET =
            new PostgresOffset(Lsn.valueOf("FFFFFFFF/FFFFFFFF").asLong(), null, Instant.MAX);

    // the offset abstract class has a protected field Map<String, String> offset
    // - postgres requires non-string type in
    // io.debezium.connector.postgresql.PostgresOffsetContext.Loader.load
    // - this feels confusing and error prone (better to refactor cdc-base?)
    private final Map<String, ?> offset;

    // used by PostgresOffsetFactory
    PostgresOffset(Map<String, String> offsetMap) {

        String lastCommitTsStr = String.valueOf(offsetMap.get(SourceInfo.TIMESTAMP_USEC_KEY));

        this.lsn = Long.valueOf(String.valueOf(offsetMap.get(SourceInfo.LSN_KEY)));
        this.txid = this.longOffsetValue(offsetMap, SourceInfo.TXID_KEY);
        this.lastCommitTs =
                Conversions.toInstantFromMicros(
                        Long.valueOf(
                                String.valueOf(
                                        offsetMap.getOrDefault(
                                                SourceInfo.TIMESTAMP_USEC_KEY, "0"))));
        this.offset = getOffsetMap(lsn, txid, lastCommitTs);
    }

    public PostgresOffset(Long lsn, Long txId, Instant lastCommitTs) {
        this.lsn = lsn;
        this.txid = txId;
        this.lastCommitTs = lastCommitTs;
        this.offset = getOffsetMap(lsn, txId, lastCommitTs);
    }

    private Map<String, ?> getOffsetMap(Long lsn, Long txId, Instant lastCommitTs) {
        // keys are from io.debezium.connector.postgresql.PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, lsn);
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, txId);
        }
        if (lastCommitTs != null) {
            offsetMap.put(SourceInfo.TIMESTAMP_USEC_KEY, Conversions.toEpochMicros(lastCommitTs));
        }
        return offsetMap;
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
        return Lsn.valueOf(lsn);
    }

    @Override
    public int compareTo(Offset o) {
        PostgresOffset rhs = (PostgresOffset) o;
        LOG.debug("comparing {} and {}", this, rhs);
        return Lsn.valueOf(this.lsn).compareTo(Lsn.valueOf(rhs.lsn));
    }

    @Override
    public String toString() {
        return "Offset{lsn="
                + Lsn.valueOf(lsn)
                + ", txId="
                + txid
                + ", lastCommitTs="
                + lastCommitTs
                + "]";
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

    @Override
    public Map<String, ?> getOffset() {

        return offset;
    }
}
