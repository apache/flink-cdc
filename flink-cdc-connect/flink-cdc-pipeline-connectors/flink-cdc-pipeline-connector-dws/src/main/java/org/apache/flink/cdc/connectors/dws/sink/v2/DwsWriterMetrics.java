/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.dws.sink.v2;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.util.Objects;

/** Connector-owned metrics with explicit observation boundaries. */
final class DwsWriterMetrics {

    private static final String METRIC_GROUP = "dws";

    private final Counter acceptedRecords;
    private final Counter writtenRecords;
    private final Counter failedRecords;
    private final Counter sentBytes;
    private final Counter flushCount;

    private volatile long conservativeBufferedBytes;
    private volatile long lastFlushDurationMillis;
    private volatile boolean firstAsyncFailure;

    private DwsWriterMetrics(
            Counter acceptedRecords,
            Counter writtenRecords,
            Counter failedRecords,
            Counter sentBytes,
            Counter flushCount) {
        this.acceptedRecords = Objects.requireNonNull(acceptedRecords);
        this.writtenRecords = Objects.requireNonNull(writtenRecords);
        this.failedRecords = Objects.requireNonNull(failedRecords);
        this.sentBytes = Objects.requireNonNull(sentBytes);
        this.flushCount = Objects.requireNonNull(flushCount);
    }

    static DwsWriterMetrics registered(SinkWriterMetricGroup metricGroup) {
        MetricGroup dwsGroup = metricGroup.addGroup(METRIC_GROUP);
        DwsWriterMetrics metrics =
                new DwsWriterMetrics(
                        dwsGroup.counter("acceptedRecords"),
                        metricGroup.getNumRecordsSendCounter(),
                        metricGroup.getNumRecordsSendErrorsCounter(),
                        metricGroup.getNumBytesSendCounter(),
                        dwsGroup.counter("flushCount"));
        dwsGroup.gauge("conservativeBufferedBytes", metrics::conservativeBufferedBytes);
        dwsGroup.gauge("lastFlushDurationMillis", metrics::lastFlushDurationMillis);
        dwsGroup.gauge("firstAsyncFailure", () -> metrics.hasFirstAsyncFailure() ? 1 : 0);
        dwsGroup.gauge("bufferAccounting", () -> "conservative-estimate");
        return metrics;
    }

    static DwsWriterMetrics testing() {
        return new DwsWriterMetrics(
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter(),
                new SimpleCounter());
    }

    void recordAccepted(long bufferedBytes) {
        acceptedRecords.inc();
        conservativeBufferedBytes = bufferedBytes;
    }

    void recordDefiniteFailure() {
        failedRecords.inc();
    }

    void recordFirstAsyncFailure() {
        firstAsyncFailure = true;
    }

    void recordSuccessfulFlush(long records, long bytes, long durationMillis) {
        writtenRecords.inc(records);
        sentBytes.inc(bytes);
        flushCount.inc();
        lastFlushDurationMillis = durationMillis;
        conservativeBufferedBytes = 0L;
    }

    long acceptedRecords() {
        return acceptedRecords.getCount();
    }

    long writtenRecords() {
        return writtenRecords.getCount();
    }

    long failedRecords() {
        return failedRecords.getCount();
    }

    long sentBytes() {
        return sentBytes.getCount();
    }

    long flushCount() {
        return flushCount.getCount();
    }

    long conservativeBufferedBytes() {
        return conservativeBufferedBytes;
    }

    long lastFlushDurationMillis() {
        return lastFlushDurationMillis;
    }

    boolean hasFirstAsyncFailure() {
        return firstAsyncFailure;
    }
}
