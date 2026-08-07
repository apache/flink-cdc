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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;

import io.debezium.connector.mysql.GtidSet;

/**
 * Calculator for binlog lag between the current consumed offset and the latest master offset.
 * Produces two independent lag values:
 *
 * <ul>
 *   <li>Transaction lag: number of GTID transactions behind (GTID mode only)
 *   <li>Byte position lag: byte offset difference (file-position mode)
 * </ul>
 */
@Internal
public class BinlogLagCalculator {

    /** Cached parsed master GtidSet to avoid re-parsing on every calculation. */
    private String cachedMasterGtidSetStr;

    private GtidSet cachedMasterGtidSet;

    /** Result of a lag calculation containing both transaction and byte position lags. */
    public static class LagResult {
        private final long transactionLag;
        private final long bytePositionLag;

        public LagResult(long transactionLag, long bytePositionLag) {
            this.transactionLag = transactionLag;
            this.bytePositionLag = bytePositionLag;
        }

        /** Transaction lag (GTID-based). Returns -1 when GTID info is unavailable. */
        public long getTransactionLag() {
            return transactionLag;
        }

        /**
         * Byte position lag (file+position based). Returns -1 when position info is unavailable.
         */
        public long getBytePositionLag() {
            return bytePositionLag;
        }
    }

    /**
     * Calculate binlog lag between current offset and master offset.
     *
     * @param current the current consumed binlog offset
     * @param master the latest master binlog offset
     * @return a LagResult with independent transaction and byte position lags
     */
    public LagResult calculateLag(BinlogOffset current, BinlogOffset master) {
        long transactionLag = calculateTransactionLag(current, master);
        long bytePositionLag = calculateBytePositionLag(current, master);
        return new LagResult(transactionLag, bytePositionLag);
    }

    private long calculateTransactionLag(BinlogOffset current, BinlogOffset master) {
        String masterGtidSetStr = master.getGtidSet();
        String currentGtidSetStr = current.getGtidSet();
        if (masterGtidSetStr == null
                || masterGtidSetStr.isEmpty()
                || currentGtidSetStr == null
                || currentGtidSetStr.isEmpty()) {
            return -1L;
        }
        if (masterGtidSetStr.equals(currentGtidSetStr)) {
            return 0L;
        }
        // Cache parsed master GtidSet
        if (!masterGtidSetStr.equals(cachedMasterGtidSetStr)) {
            cachedMasterGtidSet = new GtidSet(masterGtidSetStr);
            cachedMasterGtidSetStr = masterGtidSetStr;
        }
        GtidSet masterSet = cachedMasterGtidSet;
        GtidSet currentSet = new GtidSet(currentGtidSetStr);
        long lag = 0;
        for (GtidSet.UUIDSet masterUuidSet : masterSet.getUUIDSets()) {
            long masterMax = getMaxTransactionId(masterUuidSet);
            GtidSet.UUIDSet currentUuidSet = currentSet.forServerWithId(masterUuidSet.getUUID());
            if (currentUuidSet == null) {
                lag += masterMax;
            } else {
                long currentMax = getMaxTransactionId(currentUuidSet);
                lag += Math.max(0, masterMax - currentMax);
            }
        }
        return lag;
    }

    private long calculateBytePositionLag(BinlogOffset current, BinlogOffset master) {
        String masterFile = master.getFilename();
        String currentFile = current.getFilename();
        if (masterFile == null || currentFile == null) {
            return -1L;
        }
        if (masterFile.equals(currentFile)) {
            return Math.max(0, master.getPosition() - current.getPosition());
        }
        // Cross-file: estimate using file sequence gap
        try {
            long masterSeq = extractFileSequence(masterFile);
            long currentSeq = extractFileSequence(currentFile);
            if (masterSeq > currentSeq) {
                // Rough estimate: use 1,000,000 as synthetic weight per file gap.
                // This is NOT actual byte lag but provides a monotonically decreasing
                // indicator as the reader catches up.
                long estimatedLag =
                        (masterSeq - currentSeq) * 1_000_000L
                                + master.getPosition()
                                - current.getPosition();
                return Math.max(0, estimatedLag);
            }
        } catch (NumberFormatException e) {
            // ignore parse error
        }
        return -1L;
    }

    @VisibleForTesting
    static long extractFileSequence(String binlogFilename) {
        // binlog filename format: mysql-bin.000003
        int dotIndex = binlogFilename.lastIndexOf('.');
        if (dotIndex >= 0) {
            return Long.parseLong(binlogFilename.substring(dotIndex + 1));
        }
        return 0L;
    }

    private long getMaxTransactionId(GtidSet.UUIDSet uuidSet) {
        long max = 0;
        for (GtidSet.Interval interval : uuidSet.getIntervals()) {
            max = Math.max(max, interval.getEnd());
        }
        return max;
    }
}
