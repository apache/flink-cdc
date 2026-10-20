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

package org.apache.flink.cdc.connectors.fluss.sink.v2.metrics;

import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.testutils.MetricListener;

import org.apache.fluss.metrics.MeterView;
import org.apache.fluss.metrics.groups.GenericMetricGroup;
import org.apache.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import org.apache.fluss.testutils.common.ScheduledTask;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link WrapperFlussMetricRegistry}. */
class WrapperFlussMetricRegistryTest {

    @Test
    void testMetricViewIsUpdatedAndRemoved() throws Exception {
        MetricListener metricListener = new MetricListener();
        ManuallyTriggeredScheduledExecutorService viewUpdaterExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        WrapperFlussMetricRegistry metricRegistry =
                new WrapperFlussMetricRegistry(
                        metricListener.getMetricGroup(),
                        Collections.emptySet(),
                        viewUpdaterExecutor);
        GenericMetricGroup metricGroup = new GenericMetricGroup(metricRegistry, null, "reader");
        MeterView meterView = new MeterView(5);

        metricGroup.meter("recordsRate", meterView);
        Optional<Meter> registeredMeter = metricListener.getMeter("fluss", "reader", "recordsRate");
        assertThat(registeredMeter).isPresent();

        meterView.markEvent();
        runViewUpdater(viewUpdaterExecutor);
        runViewUpdater(viewUpdaterExecutor);
        assertThat(registeredMeter.get().getRate()).isEqualTo(0.2);

        metricRegistry.unregister(meterView, "recordsRate", metricGroup);
        runViewUpdater(viewUpdaterExecutor);
        meterView.markEvent(5);
        runViewUpdater(viewUpdaterExecutor);
        assertThat(registeredMeter.get().getRate()).isZero();

        metricRegistry.closeAsync().join();
        assertThat(viewUpdaterExecutor.isTerminated()).isTrue();
    }

    private static void runViewUpdater(
            ManuallyTriggeredScheduledExecutorService viewUpdaterExecutor) throws Exception {
        ScheduledTask<?> updaterTask =
                (ScheduledTask<?>) viewUpdaterExecutor.getAllPeriodicScheduledTask().get(0);
        updaterTask.getCallable().call();
    }
}
