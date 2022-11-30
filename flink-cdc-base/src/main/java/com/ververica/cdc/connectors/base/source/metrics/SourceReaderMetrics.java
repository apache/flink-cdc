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

package com.ververica.cdc.connectors.base.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;

/** A collection class for handling metrics in {@link IncrementalSourceReader}. */
public class SourceReaderMetrics {

    private final MetricGroup metricGroup;

    /**
     * The last record processing time, which is updated after {@link IncrementalSourceReader}
     * fetches a batch of data. It's mainly used to report metrics sourceIdleTime for sourceIdleTime
     * = System.currentTimeMillis() - processTime.
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

    public SourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                SourceReaderMetricConstants.CURRENT_FETCH_EVENT_TIME_LAG,
                (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(
                SourceReaderMetricConstants.CURRENT_EMIT_EVENT_TIME_LAG,
                (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge(
                SourceReaderMetricConstants.SOURCE_IDLE_TIME, (Gauge<Long>) this::getIdleTime);
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

    public void recordProcessTime(long processTime) {
        this.processTime = processTime;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordEmitDelay(long emitDelay) {
        this.emitDelay = emitDelay;
    }
}
