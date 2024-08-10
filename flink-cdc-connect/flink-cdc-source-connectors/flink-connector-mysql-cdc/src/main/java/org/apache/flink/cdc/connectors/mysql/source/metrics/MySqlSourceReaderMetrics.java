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
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;

import io.debezium.relational.TableId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** A collection class for handling metrics in {@link MySqlSourceReader}. */
public class MySqlSourceReaderMetrics {

    public static final long UNDEFINED = -1;

    private final MetricGroup metricGroup;

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = UNDEFINED;

    private final Map<Tuple2<TableId, OperationType>, Counter> numRecordsOutByDataChangeRecordMap =
            new ConcurrentHashMap();
    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_INSERT =
            "numRecordsOutByDataChangeRecordInsert";
    public static final String IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_INSERT =
            "numRecordsOutByPerSecondDataChangeRecordInsert";
    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_UPDATE =
            "numRecordsOutByDataChangeRecordUpdate";
    public static final String IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_UPDATE =
            "numRecordsOutByPerSecondDataChangeRecordUpdate";
    public static final String IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_DELETE =
            "numRecordsOutByDataChangeRecordDelete";
    public static final String IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_DELETE =
            "numRecordsOutByPerSecondDataChangeRecordDelete";

    public MySqlSourceReaderMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
        this.metricGroup.meter(
                IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_INSERT,
                new MeterView(metricGroup.counter(IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_INSERT)));
        this.metricGroup.meter(
                IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_UPDATE,
                new MeterView(metricGroup.counter(IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_UPDATE)));
        this.metricGroup.meter(
                IO_NUM_RECORDS_OUT_RATE_DATA_CHANGE_RECORD_DELETE,
                new MeterView(metricGroup.counter(IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_DELETE)));
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void numRecordsOutByDataChangeRecord(TableId tableId, OperationType op) {
        Counter counter =
                numRecordsOutByDataChangeRecordMap.computeIfAbsent(
                        new Tuple2<>(tableId, op),
                        k -> {
                            switch (op) {
                                case INSERT:
                                    return metricGroup.counter(
                                            IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_INSERT);
                                case UPDATE:
                                    return metricGroup.counter(
                                            IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_UPDATE);
                                case DELETE:
                                    return metricGroup.counter(
                                            IO_NUM_RECORDS_OUT_DATA_CHANGE_RECORD_DELETE);
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unsupported operation type for "
                                                    + "numRecordsOutByDataChangeRecord Metrics "
                                                    + op);
                            }
                        });

        counter.inc();
    }
}
