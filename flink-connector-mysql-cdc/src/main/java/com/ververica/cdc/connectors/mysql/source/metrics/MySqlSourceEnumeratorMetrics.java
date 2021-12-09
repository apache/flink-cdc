/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import com.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;

/** A collection class for handling metrics in {@link MySqlSourceEnumerator}. */
public class MySqlSourceEnumeratorMetrics {
    private final MetricGroup metricGroup;

    public MySqlSourceEnumeratorMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    private volatile long alreadyProcessedTablesNum = 0L;
    private volatile long remainingTablesNum = 0L;
    private volatile long remainingSplitsNum = 0L;
    private volatile long assignedSplitsNum = 0L;
    private volatile long finishedSplitsNum = 0L;
    private volatile boolean assignerFinished = false;

    public void registerMetrics() {
        metricGroup.gauge(
                "alreadyProcessedTablesNum", (Gauge<Long>) this::getAlreadyProcessedTablesNum);
        metricGroup.gauge("remainingTablesNum", (Gauge<Long>) this::getRemainingTablesNum);
        metricGroup.gauge("remainingSplitsNum", (Gauge<Long>) this::getRemainingSplitsNum);
        metricGroup.gauge("assignedSplitsNum", (Gauge<Long>) this::getAssignedSplitsNum);
        metricGroup.gauge("finishedSplitsNum", (Gauge<Long>) this::getFinishedSplitsNum);
        metricGroup.gauge("assignerFinished", (Gauge<Boolean>) this::getAssignerFinished);
    }

    public long getAlreadyProcessedTablesNum() {
        return alreadyProcessedTablesNum;
    }

    public long getRemainingTablesNum() {
        return remainingTablesNum;
    }

    public long getRemainingSplitsNum() {
        return remainingSplitsNum;
    }

    public long getAssignedSplitsNum() {
        return assignedSplitsNum;
    }

    public long getFinishedSplitsNum() {
        return finishedSplitsNum;
    }

    public boolean getAssignerFinished() {
        return assignerFinished;
    }

    public void recordAlreadyProcessedTablesNum(long alreadyProcessedTablesNum) {
        this.alreadyProcessedTablesNum = alreadyProcessedTablesNum;
    }

    public void recordRemainingTablesNum(final long remainingTablesNum) {
        this.remainingTablesNum = remainingTablesNum;
    }

    public void recordRemainingSplitsNum(long remainingSplitsNum) {
        this.remainingSplitsNum = remainingSplitsNum;
    }

    public void recordAssignedSplits(long assignedSplitsNum) {
        this.assignedSplitsNum = assignedSplitsNum;
    }

    public void recordFinishedSplits(long finishedSplitsNum) {
        this.finishedSplitsNum = finishedSplitsNum;
    }

    public void recordAssignerFinished(boolean assignerFinished) {
        this.assignerFinished = assignerFinished;
    }
}
