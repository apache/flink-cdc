/*
 * Copyright 2023 Ververica Inc.
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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;

/** A collection class for handling metrics in {@link IncrementalSourceReader}. */
public class SourceReaderMetrics {

    private final SourceReaderMetricGroup metricGroup;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /** The total number of record that failed to consume, process or emit. */
    private final Counter numRecordsInErrorsCounter;

    public SourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsInErrorsCounter = metricGroup.getNumRecordsInErrorsCounter();
    }

    public void registerMetrics() {
        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void addNumRecordsInErrors(long delta) {
        this.numRecordsInErrorsCounter.inc(delta);
    }
}
