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

package org.apache.flink.cdc.connectors.base.source.assigner;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.cdc.connectors.base.config.SourceConfig;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;
import org.apache.flink.cdc.connectors.base.source.meta.split.SourceSplitBase;
import org.apache.flink.cdc.connectors.base.source.metrics.SourceEnumeratorMetrics;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;

import static org.mockito.ArgumentMatchers.any;

/** Tests for {@link HybridSplitAssigner}. */
class HybridSplitAssignerTest {

    @Test
    void testInitializeEnumeratorMetricsBeforeOpeningSnapshotSplitAssigner() throws Exception {
        SnapshotSplitAssigner<SourceConfig> snapshotSplitAssigner = mockSnapshotSplitAssigner();
        SplitEnumeratorContext<SourceSplitBase> enumeratorContext = mockEnumeratorContext();
        HybridSplitAssigner<SourceConfig> hybridSplitAssigner =
                createHybridSplitAssigner(snapshotSplitAssigner, enumeratorContext);

        hybridSplitAssigner.open();

        InOrder inOrder = Mockito.inOrder(snapshotSplitAssigner);
        inOrder.verify(snapshotSplitAssigner)
                .initEnumeratorMetrics(any(SourceEnumeratorMetrics.class));
        inOrder.verify(snapshotSplitAssigner).open();
    }

    @SuppressWarnings("unchecked")
    private SnapshotSplitAssigner<SourceConfig> mockSnapshotSplitAssigner() {
        return Mockito.mock(SnapshotSplitAssigner.class);
    }

    @SuppressWarnings("unchecked")
    private SplitEnumeratorContext<SourceSplitBase> mockEnumeratorContext() {
        SplitEnumeratorContext<SourceSplitBase> enumeratorContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumeratorContext.metricGroup())
                .thenReturn(UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup());
        return enumeratorContext;
    }

    @SuppressWarnings("unchecked")
    private HybridSplitAssigner<SourceConfig> createHybridSplitAssigner(
            SnapshotSplitAssigner<SourceConfig> snapshotSplitAssigner,
            SplitEnumeratorContext<SourceSplitBase> enumeratorContext)
            throws Exception {
        Constructor<?> constructor =
                HybridSplitAssigner.class.getDeclaredConstructor(
                        SourceConfig.class,
                        SnapshotSplitAssigner.class,
                        boolean.class,
                        int.class,
                        OffsetFactory.class,
                        SplitEnumeratorContext.class);
        constructor.setAccessible(true);
        return (HybridSplitAssigner<SourceConfig>)
                constructor.newInstance(
                        Mockito.mock(SourceConfig.class),
                        snapshotSplitAssigner,
                        false,
                        1,
                        Mockito.mock(OffsetFactory.class),
                        enumeratorContext);
    }
}
