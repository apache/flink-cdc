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

package org.apache.flink.cdc.connectors.tidb.metrics;

import org.apache.flink.cdc.connectors.tidb.TiKVRichParallelSourceFunction;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import static org.apache.flink.runtime.metrics.MetricNames.CURRENT_EMIT_EVENT_TIME_LAG;
import static org.apache.flink.runtime.metrics.MetricNames.CURRENT_FETCH_EVENT_TIME_LAG;
import static org.apache.flink.runtime.metrics.MetricNames.SOURCE_IDLE_TIME;

/** A collection class for handling metrics in {@link TiKVRichParallelSourceFunction}. */
public class TiDBSourceMetrics {

    private final MetricGroup metricGroup;

    /**
     * The last record processing time, which is updated after {@link
     * TiKVRichParallelSourceFunction} fetches a batch of data. It's mainly used to report metrics
     * sourceIdleTime for sourceIdleTime = System.currentTimeMillis() - processTime.
     */
    private long processTime = 0L;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private long fetchDelay = 0L;

    /**
     * currentEmitEventTimeLag = EmitTime - messageTimestamp, where the EmitTime is the time the
     * record leaves the source operator.
     */
    private long emitDelay = 0L;

    public TiDBSourceMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {

        metricGroup.gauge(CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(CURRENT_EMIT_EVENT_TIME_LAG, (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge(SOURCE_IDLE_TIME, (Gauge<Long>) this::getIdleTime);
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
