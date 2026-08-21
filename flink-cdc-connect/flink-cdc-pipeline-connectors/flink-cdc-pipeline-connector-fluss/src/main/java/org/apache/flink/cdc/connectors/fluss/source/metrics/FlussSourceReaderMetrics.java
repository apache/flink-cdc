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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import org.apache.fluss.metadata.TableBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A collection class for handling metrics in the Fluss CDC source reader.
 *
 * <p>All metrics of the source reader are registered under group "fluss.reader", which is a child
 * group of {@link org.apache.flink.metrics.groups.OperatorMetricGroup}. Metrics related to a
 * specific table bucket will be registered in the group:
 *
 * <p>"fluss.reader.bucket.{bucket_id}" for non-partitioned bucket or
 * "fluss.reader.partition.{partition_id}.bucket.{bucket_id}" for partitioned bucket.
 *
 * <p>For example, current consuming offset of bucket 1 will be reported in metric:
 * "{some_parent_groups}.operator.fluss.reader.bucket.1.currentOffset"
 */
public class FlussSourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(FlussSourceReaderMetrics.class);

    // Constants
    public static final String FLUSS_METRIC_GROUP = "fluss";
    public static final String READER_METRIC_GROUP = "reader";
    public static final String PARTITION_GROUP = "partition";
    public static final String BUCKET_GROUP = "bucket";
    public static final String CURRENT_OFFSET_METRIC_GAUGE = "currentOffset";

    public static final long INITIAL_OFFSET = -1;
    public static final long UNINITIALIZED = -1;

    // Source reader metric group
    private final SourceReaderMetricGroup sourceReaderMetricGroup;

    // Metric group for registering Fluss specific reader metrics
    private final MetricGroup flussSourceReaderMetricGroup;

    // Map for tracking current consuming offsets
    private final Map<TableBucket, Long> offsets = new HashMap<>();

    // For currentFetchEventTimeLag metric
    private volatile long currentFetchEventTimeLag = UNINITIALIZED;

    public FlussSourceReaderMetrics(SourceReaderMetricGroup sourceReaderMetricGroup) {
        this.sourceReaderMetricGroup = sourceReaderMetricGroup;
        this.flussSourceReaderMetricGroup =
                sourceReaderMetricGroup.addGroup(FLUSS_METRIC_GROUP).addGroup(READER_METRIC_GROUP);
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

    public void registerTableBucket(TableBucket tableBucket) {
        offsets.put(tableBucket, INITIAL_OFFSET);
        registerOffsetMetricsForTableBucket(tableBucket);
    }

    /**
     * Update current consuming offset of the given {@link TableBucket}.
     *
     * @param tb Updating table bucket
     * @param offset Current consuming offset
     */
    public void recordCurrentOffset(TableBucket tb, long offset) {
        checkTableBucketTracked(tb);
        offsets.put(tb, offset);
    }

    // -------- Helper functions --------
    private void registerOffsetMetricsForTableBucket(TableBucket tableBucket) {
        final MetricGroup metricGroup =
                tableBucket.getPartitionId() == null
                        ? this.flussSourceReaderMetricGroup
                        : this.flussSourceReaderMetricGroup.addGroup(
                                PARTITION_GROUP, String.valueOf(tableBucket.getPartitionId()));
        final MetricGroup bucketGroup =
                metricGroup.addGroup(BUCKET_GROUP, String.valueOf(tableBucket.getBucket()));
        bucketGroup.gauge(
                CURRENT_OFFSET_METRIC_GAUGE,
                () -> offsets.getOrDefault(tableBucket, INITIAL_OFFSET));
    }

    private void checkTableBucketTracked(TableBucket tableBucket) {
        if (!offsets.containsKey(tableBucket)) {
            LOG.warn("Offset metrics of TableBucket {} is not tracked", tableBucket);
        }
    }

    public SourceReaderMetricGroup getSourceReaderMetricGroup() {
        return sourceReaderMetricGroup;
    }
}
