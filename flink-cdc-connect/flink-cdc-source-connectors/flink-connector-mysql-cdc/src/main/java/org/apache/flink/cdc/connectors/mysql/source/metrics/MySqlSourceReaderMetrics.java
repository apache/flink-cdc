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

package org.apache.flink.cdc.connectors.mysql.source.metrics;

import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    public static final String CURRENT_BINLOG_TRANSACTION_LAG = "currentBinlogTransactionLag";
    public static final String CURRENT_BINLOG_BYTE_POSITION_LAG = "currentBinlogBytePositionLag";
    public static final long UNDEFINED = -1;

    private final MetricGroup metricGroup;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = UNDEFINED;

    /**
     * The binlog transaction lag between current consumed offset and the latest master offset. This
     * metric is meaningful only in GTID mode. Reports -1 when GTID is not available.
     */
    private volatile long binlogTransactionLag = UNDEFINED;

    /**
     * The binlog byte position lag between current consumed offset and the latest master offset.
     * Reports exact byte difference for same-file comparison and an estimate for cross-file.
     * Reports -1 when position information is not available.
     */
    private volatile long binlogBytePositionLag = UNDEFINED;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
    }

    public void registerBinlogLagMetrics() {
        metricGroup.gauge(
                CURRENT_BINLOG_TRANSACTION_LAG, (Gauge<Long>) this::getBinlogTransactionLag);
        metricGroup.gauge(
                CURRENT_BINLOG_BYTE_POSITION_LAG, (Gauge<Long>) this::getBinlogBytePositionLag);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public long getBinlogTransactionLag() {
        return binlogTransactionLag;
    }

    public void recordBinlogTransactionLag(long lag) {
        this.binlogTransactionLag = lag;
    }

    public long getBinlogBytePositionLag() {
        return binlogBytePositionLag;
    }

    public void recordBinlogBytePositionLag(long lag) {
        this.binlogBytePositionLag = lag;
    }
}
