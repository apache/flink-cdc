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

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Utils for handling {@link PostgresOffset}. */
public class PostgresOffsetUtils {

    /**
     * {@code TransactionContext.OFFSET_TRANSACTION_ID}, which is not public in every Debezium
     * version this connector has been built against.
     */
    private static final String OFFSET_TRANSACTION_ID = "transaction_id";

    /** Keys that {@link PostgresOffsetContext.Loader#load} reads as a {@code Boolean}. */
    private static final Set<String> BOOLEAN_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            AbstractSourceInfo.SNAPSHOT_KEY, SourceInfo.LAST_SNAPSHOT_RECORD_KEY));

    /**
     * Keys that are read as a {@code String} even though their value may look numeric. Debezium 2.1
     * added {@code messageType} to the Postgres offset, which is where the previous "every value is
     * a long" assumption broke; {@code transaction_id} is a numeric Postgres txid that must still
     * be kept as a String.
     */
    private static final Set<String> STRING_KEYS =
            new HashSet<>(Arrays.asList(SourceInfo.MSG_TYPE_KEY, OFFSET_TRANSACTION_ID));

    /**
     * The incremental snapshot context stores serialized keys and collection names under this
     * prefix and always reads them back as Strings.
     */
    private static final String INCREMENTAL_SNAPSHOT_PREFIX =
            AbstractIncrementalSnapshotContext.INCREMENTAL_SNAPSHOT_KEY;

    /**
     * The one key under {@link #INCREMENTAL_SNAPSHOT_PREFIX} that is read back as a number, used
     * only by the read-only incremental snapshot context.
     */
    private static final String INCREMENTAL_SNAPSHOT_SIGNAL_OFFSET =
            INCREMENTAL_SNAPSHOT_PREFIX + "_signal_offset";

    public static PostgresOffsetContext getPostgresOffsetContext(
            PostgresOffsetContext.Loader loader, Offset offset) {

        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // The offset was flattened to strings by PostgresOffset; restore the types that
        // PostgresOffsetContext.Loader.load expects. Numeric-looking values default to Long
        // (lsn, txId, ts_usec, per-table event counts, ...), everything else stays a String.
        Map<String, Object> offsetMap = new HashMap<>();
        for (Map.Entry<String, String> entry : offsetStrMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (BOOLEAN_KEYS.contains(key)) {
                offsetMap.put(key, Boolean.parseBoolean(value));
            } else if (STRING_KEYS.contains(key)
                    || (key.startsWith(INCREMENTAL_SNAPSHOT_PREFIX)
                            && !INCREMENTAL_SNAPSHOT_SIGNAL_OFFSET.equals(key))) {
                offsetMap.put(key, value);
            } else {
                offsetMap.put(key, toLongOrKeepString(value));
            }
        }
        return loader.load(offsetMap);
    }

    private static Object toLongOrKeepString(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
