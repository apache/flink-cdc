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

import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import io.debezium.relational.TableId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    public static final long UNDEFINED = -1;

    private final MetricGroup metricGroup;

    public static final String IO_NUM_RECORDS_OUT_SNAPSHOT = ".numRecordsOutBySnapshot";

    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_INSERT =
            ".numRecordsOutByDataChangeEventInsert";

    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_DELETE =
            ".numRecordsOutByDataChangeEventDelete";

    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_UPDATE =
            ".numRecordsOutByDataChangeEventUpdate";

    private final Map<TableId, Counter> snapshotNumRecordsOutMap = new ConcurrentHashMap();

    private final Map<TableId, Counter> insertNumRecordsOutMap = new ConcurrentHashMap();

    private final Map<TableId, Counter> updateNumRecordsOutMap = new ConcurrentHashMap();

    private final Map<TableId, Counter> deleteNumRecordsOutMap = new ConcurrentHashMap();

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = UNDEFINED;

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
    }

    public void numRecordsOutSnapshotIncrease(TableId tableId) {
        Counter counter =
                snapshotNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        tableId.identifier() + IO_NUM_RECORDS_OUT_SNAPSHOT));
        counter.inc();
    }

    public void numRecordsOutInsertIncrease(TableId tableId) {
        Counter counter =
                insertNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        tableId.identifier()
                                                + IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_INSERT));
        counter.inc();
    }

    public void numRecordsOutUpdateIncrease(TableId tableId) {
        Counter counter =
                updateNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        tableId.identifier()
                                                + IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_UPDATE));
        counter.inc();
    }

    public void numRecordsOutDeleteIncrease(TableId tableId) {
        Counter counter =
                deleteNumRecordsOutMap.computeIfAbsent(
                        tableId,
                        k ->
                                metricGroup.counter(
                                        tableId.identifier()
                                                + IO_NUM_RECORDS_OUT_DATA_CHANGE_EVENT_DELETE));
        counter.inc();
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }
}
