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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.ververica.cdc.connectors.base.source.reader.IncrementalSourceReader;

import java.util.concurrent.atomic.AtomicLong;

/** A collection class for handling metrics in {@link IncrementalSourceReader}. */
public class SourceReaderMetrics {

    private final MetricGroup metricGroup;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = 0L;

    /**
     * The number of records that have not been fetched by the source. e.g. the available records
     * after the consumer offset in a Kafka partition.
     */
    private volatile long pendingRecords = 0L;

    /** The total number of record that failed to consume, process or emit. */
    private volatile AtomicLong numRecordsInErrors = new AtomicLong(0L);

    public SourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                SourceReaderMetricConstants.CURRENT_FETCH_EVENT_TIME_LAG,
                (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(
                SourceReaderMetricConstants.PENDING_RECORDS, (Gauge<Long>) this::getPendingRecords);
        metricGroup.gauge(
                SourceReaderMetricConstants.NUM_RECORDS_IN_ERRORS,
                (Gauge<Long>) this::getNumRecordsInErrors);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public long getPendingRecords() {
        return pendingRecords;
    }

    public long getNumRecordsInErrors() {
        return numRecordsInErrors.get();
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordPendingRecords(long pendingRecords) {
        this.pendingRecords = pendingRecords;
    }

    public void recordNumRecordsInErrors(long numRecordsInErrors) {
        this.numRecordsInErrors.set(numRecordsInErrors);
    }

    public long addNumRecordsInErrors(long delta) {
        return this.numRecordsInErrors.getAndAdd(delta);
    }
}
