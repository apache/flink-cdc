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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.apache.flink.runtime.metrics.MetricNames.CURRENT_EMIT_EVENT_TIME_LAG;
import static org.apache.flink.runtime.metrics.MetricNames.CURRENT_FETCH_EVENT_TIME_LAG;

/** Unit test for {@link TiDBSourceMetrics}. */
class TiDBSourceMetricsTest {
    private MetricListener metricListener;
    private TiDBSourceMetrics sourceMetrics;

    @BeforeEach
    public void setUp() {
        metricListener = new MetricListener();
        sourceMetrics = new TiDBSourceMetrics(metricListener.getMetricGroup());
        sourceMetrics.registerMetrics();
    }

    @Test
    void testFetchEventTimeLagTracking() {
        sourceMetrics.recordFetchDelay(5L);
        assertGauge(metricListener, CURRENT_FETCH_EVENT_TIME_LAG, 5L);
    }

    @Test
    void testEmitEventTimeLagTracking() {
        sourceMetrics.recordEmitDelay(3L);
        assertGauge(metricListener, CURRENT_EMIT_EVENT_TIME_LAG, 3L);
    }

    private void assertGauge(MetricListener metricListener, String identifier, long expected) {
        Optional<Gauge<Object>> gauge = metricListener.getGauge(identifier);
        Assertions.assertThat(gauge).map(Gauge::getValue).hasValue(expected);
    }
}
