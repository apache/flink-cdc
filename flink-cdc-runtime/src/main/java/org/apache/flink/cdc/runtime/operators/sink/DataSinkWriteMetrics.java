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

package org.apache.flink.cdc.runtime.operators.sink;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A collection class for handling metrics in {@link DataSinkWriterOperator}. */
public class DataSinkWriteMetrics {

    private final MetricGroup metricGroup;

    public static final String IO_NUM_RECORDS_OUT_INSERT = "numRecordsOutByInsert";

    public static final String IO_NUM_RECORDS_OUT_DELETE = "numRecordsOutByDelete";

    public static final String IO_NUM_RECORDS_OUT_UPDATE = "numRecordsOutByUpdate";

    private final Map<TableId, Counter> insertNumRecordsOutMap = new ConcurrentHashMap();

    private final Map<TableId, Counter> updateNumRecordsOutMap = new ConcurrentHashMap();

    private final Map<TableId, Counter> deleteNumRecordsOutMap = new ConcurrentHashMap();

    public DataSinkWriteMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void numRecordsOutInsertIncrease(TableId tableId) {
        Counter counter =
                insertNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        String.format(
                                                "%s_%s",
                                                tableId.identifier(), IO_NUM_RECORDS_OUT_INSERT)));
        counter.inc();
    }

    public void numRecordsOutUpdateIncrease(TableId tableId) {
        Counter counter =
                updateNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        String.format(
                                                "%s_%s",
                                                tableId.identifier(), IO_NUM_RECORDS_OUT_UPDATE)));
        counter.inc();
    }

    public void numRecordsOutDeleteIncrease(TableId tableId) {
        Counter counter =
                deleteNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        String.format(
                                                "%s_%s",
                                                tableId.identifier(), IO_NUM_RECORDS_OUT_DELETE)));
        counter.inc();
    }
}
