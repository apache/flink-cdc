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

package org.apache.flink.cdc.connectors.tdengine.sink;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import javax.annotation.Nullable;

/** Metrics for TDengine sink writer. */
class TDengineSinkMetrics {

    static final TDengineSinkMetrics NOOP = new TDengineSinkMetrics(null);

    private final Counter recordsOut;
    private final Counter recordsOutErrors;
    private final Counter flushCount;
    private final Counter flushFailureCount;
    private final Counter retryCount;
    private final Counter reconnectCount;

    private volatile long pendingRows;
    private volatile long lastFlushDurationMillis;

    TDengineSinkMetrics(@Nullable SinkWriterMetricGroup metricGroup) {
        if (metricGroup == null) {
            this.recordsOut = NoOpCounter.INSTANCE;
            this.recordsOutErrors = NoOpCounter.INSTANCE;
            this.flushCount = NoOpCounter.INSTANCE;
            this.flushFailureCount = NoOpCounter.INSTANCE;
            this.retryCount = NoOpCounter.INSTANCE;
            this.reconnectCount = NoOpCounter.INSTANCE;
            return;
        }
        this.recordsOut = metricGroup.getNumRecordsSendCounter();
        this.recordsOutErrors = metricGroup.getNumRecordsOutErrorsCounter();
        this.flushCount = metricGroup.counter("tdengineFlushCount");
        this.flushFailureCount = metricGroup.counter("tdengineFlushFailureCount");
        this.retryCount = metricGroup.counter("tdengineRetryCount");
        this.reconnectCount = metricGroup.counter("tdengineReconnectCount");
        metricGroup.gauge("tdenginePendingRows", () -> pendingRows);
        metricGroup.gauge("tdengineLastFlushDurationMs", () -> lastFlushDurationMillis);
        metricGroup.setCurrentSendTimeGauge(() -> lastFlushDurationMillis);
    }

    void setPendingRows(long pendingRows) {
        this.pendingRows = pendingRows;
    }

    void recordSuccessfulFlush(long rows, long durationMillis) {
        flushCount.inc();
        recordsOut.inc(rows);
        lastFlushDurationMillis = durationMillis;
        setPendingRows(0);
    }

    void recordFlushFailure() {
        flushFailureCount.inc();
    }

    void recordRecordsOutErrors(long rows) {
        recordsOutErrors.inc(rows);
    }

    void recordRetry() {
        retryCount.inc();
    }

    void recordReconnect() {
        reconnectCount.inc();
    }

    private enum NoOpCounter implements Counter {
        INSTANCE;

        @Override
        public void inc() {}

        @Override
        public void inc(long n) {}

        @Override
        public void dec() {}

        @Override
        public void dec(long n) {}

        @Override
        public long getCount() {
            return 0;
        }
    }
}
