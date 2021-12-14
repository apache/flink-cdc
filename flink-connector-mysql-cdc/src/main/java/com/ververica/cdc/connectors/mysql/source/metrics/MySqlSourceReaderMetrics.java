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

package com.ververica.cdc.connectors.mysql.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    private final MetricGroup metricGroup;

    /**
     * The last record processing time, which is updated after {@link MySqlSourceReader} fetches a
     * batch of data. It's mainly used to report metrics sourceIdleTime for sourceIdleTime =
     * System.currentTimeMillis() - processTime.
     */
    private volatile long processTime = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
     * source operator.
     */
    private volatile long emitDelay = 0L;

    /** Already processed table count for this flink Job. */
    private volatile long alreadyProcessedTableCount = 0L;

    /** Remaining table count for this flink job to handle. */
    private volatile long remainingTableCount = 0L;

    /** Already assigned split count for this flink Job. */
    private volatile long assignedSplitCount = 0L;

    /** Remaining split count for the table int process. */
    private volatile long remainingSplitCount = 0L;

    /** Already processed split count for this flink Job. */
    private volatile long finishedSplitCount = 0L;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge("currentFetchEventTimeLag", (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge("currentEmitEventTimeLag", (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge("sourceIdleTime", (Gauge<Long>) this::getIdleTime);
        metricGroup.gauge(
                "alreadyProcessedTableCount", (Gauge<Long>) this::getAlreadyProcessedTableCount);
        metricGroup.gauge("remainingTableCount", (Gauge<Long>) this::getRemainingTableCount);
        metricGroup.gauge("assignedSplitCount", (Gauge<Long>) this::getAssignedSplitCount);
        metricGroup.gauge("remainingSplitCount", (Gauge<Long>) this::getRemainingSplitCount);
        metricGroup.gauge("finishedSplitCount", (Gauge<Long>) this::getFinishedSplitCount);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public long getEmitDelay() {
        return emitDelay;
    }

    public long getIdleTime() {
        // no previous process time at the beginning, return 0 as idle time
        if (processTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - processTime;
    }

    public long getAlreadyProcessedTableCount() {
        return alreadyProcessedTableCount;
    }

    public long getRemainingTableCount() {
        return remainingTableCount;
    }

    public long getAssignedSplitCount() {
        return assignedSplitCount;
    }

    public long getRemainingSplitCount() {
        return remainingSplitCount;
    }

    public long getFinishedSplitCount() {
        return finishedSplitCount;
    }

    public void recordProcessTime(long processTime) {
        this.processTime = processTime;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordEmitDelay(long emitDelay) {
        this.emitDelay = emitDelay;
    }

    public void recordAlreadyProcessedTableCount(final long alreadyProcessedTableCount) {
        this.alreadyProcessedTableCount = alreadyProcessedTableCount;
    }

    public void recordRemainingTableCount(final long remainingTableCount) {
        this.remainingTableCount = remainingTableCount;
    }

    public void recordAssignedSplitCount(final long assignedSplitCount) {
        this.assignedSplitCount = assignedSplitCount;
    }

    public void recordRemainingSplitCount(final long remainingSplitCount) {
        this.remainingSplitCount = remainingSplitCount;
    }

    public void recordFinishedSplitCount(final long finishedSplitCount) {
        this.finishedSplitCount = finishedSplitCount;
    }
}
