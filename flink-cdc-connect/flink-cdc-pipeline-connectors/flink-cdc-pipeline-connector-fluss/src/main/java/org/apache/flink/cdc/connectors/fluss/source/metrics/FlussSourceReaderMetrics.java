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

package org.apache.flink.cdc.connectors.fluss.source.metrics;

import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import javax.annotation.Nullable;

import java.util.Collections;

/** A collection class for handling metrics in the Fluss CDC source reader. */
public class FlussSourceReaderMetrics {

    public static final long UNINITIALIZED = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    @Nullable private WrapperFlussMetricRegistry splitReaderMetricRegistry;

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = UNINITIALIZED;

    public FlussSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
    }

    public WrapperFlussMetricRegistry createSplitReaderMetricRegistry() {
        // Flink reuses open subgroups by name. Close the old group first so a retiring
        // connection cannot unregister its successor's metrics.
        closeSplitReaderMetricRegistry();
        splitReaderMetricRegistry =
                new WrapperFlussMetricRegistry(sourceReaderMetricGroup, Collections.emptySet());
        return splitReaderMetricRegistry;
    }

    public void closeSplitReaderMetricRegistry() {
        if (splitReaderMetricRegistry != null) {
            splitReaderMetricRegistry.closeAsync().join();
            splitReaderMetricRegistry = null;
        }
    }

    public void reportRecordEventTime(long lag) {
        if (currentFetchEventTimeLag == UNINITIALIZED) {
            // Lazily register the currentFetchEventTimeLag
            // Set the lag before registering the metric to avoid metric reporter getting
            // the uninitialized value
            currentFetchEventTimeLag = lag;
            sourceReaderMetricGroup.gauge(
                    MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, () -> currentFetchEventTimeLag);
            return;
        }
        currentFetchEventTimeLag = lag;
    }

    public SourceReaderMetricGroup getSourceReaderMetricGroup() {
        return sourceReaderMetricGroup;
    }
}
