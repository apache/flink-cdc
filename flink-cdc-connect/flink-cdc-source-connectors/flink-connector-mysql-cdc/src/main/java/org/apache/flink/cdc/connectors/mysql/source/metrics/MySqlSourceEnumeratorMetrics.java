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

package org.apache.flink.cdc.connectors.mysql.source.metrics;

import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.metrics.MetricGroup;

/**
 * Enumerator-side metrics for the MySQL CDC source. Exposes the {@link
 * org.apache.flink.cdc.connectors.mysql.source.assigners.AssignerStatus} of the split assigner and
 * aggregate snapshot progress counters so operators can observe whether the assigner state machine
 * is making progress -- particularly when {@code scan.newly-added-table.enabled=true} keeps the
 * assigner in one of the {@code *_ASSIGNING} states for an extended snapshot.
 *
 * <p>Registered by {@link MySqlSourceEnumerator}.
 */
public class MySqlSourceEnumeratorMetrics {

    // Metric names
    public static final String ASSIGNER_STATUS = "assignerStatus";
    public static final String ASSIGNER_STATUS_NAME = "assignerStatusName";
    public static final String NUM_REMAINING_TABLES = "numRemainingTables";
    public static final String NUM_REMAINING_SNAPSHOT_SPLITS = "numRemainingSnapshotSplits";
    public static final String NUM_ASSIGNED_SNAPSHOT_SPLITS = "numAssignedSnapshotSplits";
    public static final String NUM_FINISHED_SNAPSHOT_SPLITS = "numFinishedSnapshotSplits";
    public static final String NUM_ALREADY_PROCESSED_TABLES = "numAlreadyProcessedTables";

    public MySqlSourceEnumeratorMetrics(MetricGroup metricGroup, MySqlSplitAssigner splitAssigner) {
        metricGroup.gauge(ASSIGNER_STATUS, () -> splitAssigner.getAssignerStatus().getStatusCode());
        metricGroup.gauge(ASSIGNER_STATUS_NAME, () -> splitAssigner.getAssignerStatus().name());
        metricGroup.gauge(NUM_REMAINING_TABLES, splitAssigner::getRemainingTablesCount);
        metricGroup.gauge(NUM_REMAINING_SNAPSHOT_SPLITS, splitAssigner::getRemainingSplitsCount);
        metricGroup.gauge(NUM_ASSIGNED_SNAPSHOT_SPLITS, splitAssigner::getAssignedSplitsCount);
        metricGroup.gauge(NUM_FINISHED_SNAPSHOT_SPLITS, splitAssigner::getFinishedSplitsCount);
        metricGroup.gauge(
                NUM_ALREADY_PROCESSED_TABLES, splitAssigner::getAlreadyProcessedTablesCount);
    }
}
