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

package com.ververica.cdc.connectors.sqlserver.experimental.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * A structure describes a fine grained offset in a binlog event including binlog position and gtid
 * set etc.
 *
 * <p>This structure can also be used to deal the binlog event in transaction, a transaction may
 * contains multiple change events, and each change event may contain multiple rows. When restart
 * from a specific {@link TransactionLogOffset}, we need to skip the processed change events and the
 * processed rows.
 */

public class TransactionLogOffset extends Offset {

    // TODO: 2022/5/27 sqlserver offset object
    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";
    public static final String GTID_SET_KEY = "gtids";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SERVER_ID_KEY = "server_id";

    public static final TransactionLogOffset INITIAL_OFFSET = new TransactionLogOffset("", 0);
    public static final TransactionLogOffset NO_STOPPING_OFFSET =
            new TransactionLogOffset("", Long.MIN_VALUE);

    public TransactionLogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public TransactionLogOffset(String filename, long position) {
        this(filename, position, 0L, 0L, 0L, null, null);
    }

    public TransactionLogOffset(
            String filename,
            long position,
            long restartSkipEvents,
            long restartSkipRows,
            long binlogEpochSecs,
            @Nullable String restartGtidSet,
            @Nullable Integer serverId) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(BINLOG_FILENAME_OFFSET_KEY, filename);
        offsetMap.put(BINLOG_POSITION_OFFSET_KEY, String.valueOf(position));
        offsetMap.put(EVENTS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipEvents));
        offsetMap.put(ROWS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipRows));
        offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochSecs));
        if (restartGtidSet != null) {
            offsetMap.put(GTID_SET_KEY, restartGtidSet);
        }
        if (serverId != null) {
            offsetMap.put(SERVER_ID_KEY, String.valueOf(serverId));
        }
        this.offset = offsetMap;
    }

    /**
     * This method is inspired by {@link io.debezium.relational.history.HistoryRecordComparator}.
     */
    @Override
    public int compareTo(Offset offset) {
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransactionLogOffset)) {
            return false;
        }
        TransactionLogOffset that = (TransactionLogOffset) o;
        return offset.equals(that.offset);
    }
}
