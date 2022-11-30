/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.table;

import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Debezium startup options. */
public final class StartupOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    public final StartupMode startupMode;
    @Nullable public final BinlogOffset binlogOffset;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest binlog.
     */
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the beginning of the binlog. This should be used with care, as it is only valid when the
     * binlog is guaranteed to contain the entire history of the database.
     */
    public static StartupOptions earliest() {
        return new StartupOptions(StartupMode.EARLIEST_OFFSET, BinlogOffset.ofEarliest());
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the binlog which means only have the changes since the connector was started.
     */
    public static StartupOptions latest() {
        return new StartupOptions(StartupMode.LATEST_OFFSET, BinlogOffset.ofLatest());
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified offset.
     */
    public static StartupOptions specificOffset(String specificOffsetFile, long specificOffsetPos) {
        return new StartupOptions(
                StartupMode.SPECIFIC_OFFSETS,
                BinlogOffset.ofBinlogFilePosition(specificOffsetFile, specificOffsetPos));
    }

    public static StartupOptions specificOffset(String gtidSet) {
        return new StartupOptions(StartupMode.SPECIFIC_OFFSETS, BinlogOffset.ofGtidSet(gtidSet));
    }

    public static StartupOptions specificOffset(BinlogOffset binlogOffset) {
        return new StartupOptions(StartupMode.SPECIFIC_OFFSETS, binlogOffset);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read binlog from the specified timestamp.
     *
     * <p>The consumer will traverse the binlog from the beginning and ignore change events whose
     * timestamp is smaller than the specified timestamp.
     *
     * @param startupTimestampMillis timestamp for the startup offsets, as milliseconds from epoch.
     */
    public static StartupOptions timestamp(long startupTimestampMillis) {
        return new StartupOptions(
                StartupMode.TIMESTAMP, BinlogOffset.ofTimestampSec(startupTimestampMillis / 1000));
    }

    private StartupOptions(StartupMode startupMode, BinlogOffset binlogOffset) {
        this.startupMode = startupMode;
        this.binlogOffset = binlogOffset;
        if (startupMode != StartupMode.INITIAL) {
            checkNotNull(
                    binlogOffset, "Binlog offset is required if startup mode is %s", startupMode);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode && Objects.equals(binlogOffset, that.binlogOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode, binlogOffset);
    }
}
