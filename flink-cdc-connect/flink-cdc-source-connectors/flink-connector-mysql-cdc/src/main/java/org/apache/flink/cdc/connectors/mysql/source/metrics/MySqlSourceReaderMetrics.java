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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import io.debezium.relational.TableId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    public static final long UNDEFINED = -1;
    private static final Map<OperationType, String> DATA_CHANGE_RECORD_MAP =
            new ConcurrentHashMap<OperationType, String>() {
                {
                    put(OperationType.INSERT, "DataChangeRecordInsert");
                    put(OperationType.UPDATE, "DataChangeRecordUpdate");
                    put(OperationType.DELETE, "DataChangeRecordDelete");
                }
            };

    private final MetricGroup metricGroup;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = UNDEFINED;

    private final Map<Tuple2<TableId, OperationType>, Counter> numRecordsOutByDataChangeRecordMap =
            new ConcurrentHashMap();
    private final Map<Tuple2<TableId, OperationType>, Meter>
            numRecordsOutByRateDataChangeRecordMap = new ConcurrentHashMap();

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void numRecordsOutByDataChangeRecord(TableId tableId, OperationType op) {
        Tuple2<TableId, OperationType> metricMapKey = new Tuple2<>(tableId, op);

        Counter counter =
                numRecordsOutByDataChangeRecordMap.compute(
                        metricMapKey,
                        (keyForCounter, existingCounter) -> {
                            if (existingCounter == null) {
                                Counter newCounter =
                                        metricGroup.counter(
                                                MetricNames.IO_NUM_RECORDS_OUT
                                                        + DATA_CHANGE_RECORD_MAP.get(op));
                                numRecordsOutByRateDataChangeRecordMap.computeIfAbsent(
                                        metricMapKey,
                                        keyForMeter ->
                                                metricGroup.meter(
                                                        MetricNames.IO_NUM_RECORDS_OUT_RATE
                                                                + DATA_CHANGE_RECORD_MAP.get(op),
                                                        new MeterView(newCounter)));
                                return newCounter;
                            } else {
                                return existingCounter;
                            }
                        });

        counter.inc();
    }
}
