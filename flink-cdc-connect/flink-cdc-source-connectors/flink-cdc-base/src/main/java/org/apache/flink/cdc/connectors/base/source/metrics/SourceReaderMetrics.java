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

package org.apache.flink.cdc.connectors.base.source.metrics;

import org.apache.flink.cdc.connectors.base.source.reader.IncrementalSourceReader;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.util.clock.SystemClock;

import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.getTableId;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isDataChangeRecord;
import static org.apache.flink.cdc.connectors.base.utils.SourceRecordUtils.isSchemaChangeEvent;

/** A collection class for handling metrics in {@link IncrementalSourceReader}. */
public class SourceReaderMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(SourceReaderMetrics.class);

    public static final long UNDEFINED = -1;

    // Metric group keys
    public static final String NAMESPACE_GROUP_KEY = "namespace";
    public static final String SCHEMA_GROUP_KEY = "schema";
    public static final String TABLE_GROUP_KEY = "table";

    // Metric names
    public static final String NUM_SNAPSHOT_RECORDS = "numSnapshotRecords";
    public static final String NUM_INSERT_DML_RECORDS = "numInsertDMLRecords";
    public static final String NUM_UPDATE_DML_RECORDS = "numUpdateDMLRecords";
    public static final String NUM_DELETE_DML_RECORDS = "numDeleteDMLRecords";
    public static final String NUM_DDL_RECORDS = "numDDLRecords";
    public static final String CURRENT_EVENT_TIME_LAG = "currentEventTimeLag";

    private final SourceReaderMetricGroup metricGroup;

    // Reader-level metrics
    private final Counter snapshotCounter;
    private final Counter insertCounter;
    private final Counter updateCounter;
    private final Counter deleteCounter;
    private final Counter schemaChangeCounter;

    private final Map<TableId, TableMetrics> tableMetricsMap = new HashMap<>();

    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    private volatile long fetchDelay = UNDEFINED;

    /** The total number of record that failed to consume, process or emit. */
    private final Counter numRecordsInErrorsCounter;
    /** The timestamp of the last record received. */
    private volatile long lastReceivedEventTime = UNDEFINED;

    public SourceReaderMetrics(SourceReaderMetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        this.numRecordsInErrorsCounter = metricGroup.getNumRecordsInErrorsCounter();

        metricGroup.gauge(
                MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(CURRENT_EVENT_TIME_LAG, this::getCurrentEventTimeLag);

        snapshotCounter = metricGroup.counter(NUM_SNAPSHOT_RECORDS);
        insertCounter = metricGroup.counter(NUM_INSERT_DML_RECORDS);
        updateCounter = metricGroup.counter(NUM_UPDATE_DML_RECORDS);
        deleteCounter = metricGroup.counter(NUM_DELETE_DML_RECORDS);
        schemaChangeCounter = metricGroup.counter(NUM_DDL_RECORDS);
    }

    public long getFetchDelay() {
        return fetchDelay;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void addNumRecordsInErrors(long delta) {
        this.numRecordsInErrorsCounter.inc(delta);
    }

    public void updateLastReceivedEventTime(Long eventTimestamp) {
        if (eventTimestamp != null && eventTimestamp > 0L) {
            lastReceivedEventTime = eventTimestamp;
        }
    }

    public void markRecord() {
        try {
            metricGroup.getIOMetricGroup().getNumRecordsInCounter().inc();
        } catch (Exception e) {
            LOG.warn("Failed to update record counters.", e);
        }
    }

    public void updateRecordCounters(SourceRecord record) {
        catchAndWarnLogAllExceptions(
                () -> {
                    // Increase reader and table level input counters
                    if (isDataChangeRecord(record)) {
                        TableMetrics tableMetrics = getTableMetrics(getTableId(record));
                        Envelope.Operation op = Envelope.operationFor(record);
                        switch (op) {
                            case READ:
                                snapshotCounter.inc();
                                tableMetrics.markSnapshotRecord();
                                break;
                            case CREATE:
                                insertCounter.inc();
                                tableMetrics.markInsertRecord();
                                break;
                            case DELETE:
                                deleteCounter.inc();
                                tableMetrics.markDeleteRecord();
                                break;
                            case UPDATE:
                                updateCounter.inc();
                                tableMetrics.markUpdateRecord();
                                break;
                        }
                    } else if (isSchemaChangeEvent(record)) {
                        schemaChangeCounter.inc();
                        TableId tableId = getTableId(record);
                        if (tableId != null) {
                            getTableMetrics(tableId).markSchemaChangeRecord();
                        }
                    }
                });
    }

    private TableMetrics getTableMetrics(TableId tableId) {
        return tableMetricsMap.computeIfAbsent(
                tableId,
                id -> new TableMetrics(id.catalog(), id.schema(), id.table(), metricGroup));
    }

    // ------------------------------- Helper functions -----------------------------

    private void catchAndWarnLogAllExceptions(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            // Catch all exceptions as errors in metric handling should not fail the job
            LOG.warn("Failed to update metrics", e);
        }
    }

    private long getCurrentEventTimeLag() {
        if (lastReceivedEventTime == UNDEFINED) {
            return UNDEFINED;
        }
        return SystemClock.getInstance().absoluteTimeMillis() - lastReceivedEventTime;
    }

    // ----------------------------------- Helper classes --------------------------------

    /**
     * Collection class for managing metrics of a table.
     *
     * <p>Metrics of table level are registered in its corresponding subgroup under the {@link
     * SourceReaderMetricGroup}.
     */
    private static class TableMetrics {
        // Snapshot + Stream
        private final Counter recordsCounter;

        // Snapshot phase
        private final Counter snapshotCounter;

        // Stream phase
        private final Counter insertCounter;
        private final Counter updateCounter;
        private final Counter deleteCounter;
        private final Counter schemaChangeCounter;

        public TableMetrics(
                String databaseName, String schemaName, String tableName, MetricGroup parentGroup) {
            databaseName = processNull(databaseName);
            schemaName = processNull(schemaName);
            tableName = processNull(tableName);
            MetricGroup metricGroup =
                    parentGroup
                            .addGroup(NAMESPACE_GROUP_KEY, databaseName)
                            .addGroup(SCHEMA_GROUP_KEY, schemaName)
                            .addGroup(TABLE_GROUP_KEY, tableName);
            recordsCounter = metricGroup.counter(MetricNames.IO_NUM_RECORDS_IN);
            snapshotCounter = metricGroup.counter(NUM_SNAPSHOT_RECORDS);
            insertCounter = metricGroup.counter(NUM_INSERT_DML_RECORDS);
            updateCounter = metricGroup.counter(NUM_UPDATE_DML_RECORDS);
            deleteCounter = metricGroup.counter(NUM_DELETE_DML_RECORDS);
            schemaChangeCounter = metricGroup.counter(NUM_DDL_RECORDS);
        }

        private String processNull(String name) {
            if (StringUtils.isBlank(name)) {
                // If null, convert to an empty string
                return "";
            }
            return name;
        }

        public void markSnapshotRecord() {
            recordsCounter.inc();
            snapshotCounter.inc();
        }

        public void markInsertRecord() {
            recordsCounter.inc();
            insertCounter.inc();
        }

        public void markDeleteRecord() {
            recordsCounter.inc();
            deleteCounter.inc();
        }

        public void markUpdateRecord() {
            recordsCounter.inc();
            updateCounter.inc();
        }

        public void markSchemaChangeRecord() {
            recordsCounter.inc();
            schemaChangeCounter.inc();
        }
    }
}
