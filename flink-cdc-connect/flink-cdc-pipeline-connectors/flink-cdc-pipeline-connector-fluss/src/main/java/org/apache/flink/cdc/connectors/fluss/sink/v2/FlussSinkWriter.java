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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.connectors.fluss.sink.v2.metrics.WrapperFlussMetricRegistry;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.metrics.Gauge;
import com.alibaba.fluss.metrics.Metric;
import com.alibaba.fluss.metrics.MetricNames;
import com.alibaba.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Base class for Flink {@link SinkWriter} implementations in Fluss. */
public class FlussSinkWriter<InputT> implements SinkWriter<InputT> {

    protected static final Logger LOG = LoggerFactory.getLogger(FlussSinkWriter.class);

    private final Configuration flussConfig;
    private final MailboxExecutor mailboxExecutor;
    private final FlussEventSerializer<InputT> flussRecordSerializer;

    private transient Connection connection;
    protected transient WrapperFlussMetricRegistry flinkMetricRegistry;

    protected transient SinkWriterMetricGroup metricGroup;

    private transient Counter numRecordsOutCounter;
    private transient Counter numRecordsOutErrorsCounter;
    private volatile Throwable asyncWriterException;

    private final Map<TablePath, TableWriter> writerMap;
    private final Map<TablePath, Table> tableMap;

    public FlussSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            FlussEventSerializer<InputT> flussRecordSerializer) {
        this.flussConfig = flussConfig;
        this.mailboxExecutor = mailboxExecutor;
        this.flussRecordSerializer = flussRecordSerializer;
        this.writerMap = new HashMap<>();
        this.tableMap = new HashMap<>();
    }

    public void initialize(SinkWriterMetricGroup metricGroup) throws IOException {
        LOG.info("Opening Fluss with config {}", flussConfig);
        this.metricGroup = metricGroup;
        flinkMetricRegistry =
                new WrapperFlussMetricRegistry(
                        metricGroup, Collections.singleton(MetricNames.WRITER_SEND_LATENCY_MS));
        connection = ConnectionFactory.createConnection(flussConfig, flinkMetricRegistry);
        flussRecordSerializer.open(connection);

        initMetrics();
    }

    protected void initMetrics() {
        numRecordsOutCounter = metricGroup.getNumRecordsSendCounter();
        numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
    }

    @Override
    public void write(InputT inputValue, Context context) throws IOException {
        checkAsyncException();

        try {
            FlussEvent flussEvent = flussRecordSerializer.serialize(inputValue);

            TablePath tablePath = flussEvent.getTablePath();

            if (flussEvent.isShouldRefreshSchema() || !writerMap.containsKey(tablePath)) {
                // refresh table schema
                if (tableMap.containsKey(tablePath)) {
                    Table table = tableMap.remove(tablePath);
                    writerMap.remove(tablePath);
                    table.close();
                }

                Table table = connection.getTable(tablePath);
                TableWriter writer;
                if (table.getTableInfo().hasPrimaryKey()) {
                    writer = table.newUpsert().createWriter();
                } else {
                    writer = table.newAppend().createWriter();
                }
                tableMap.put(tablePath, table);
                writerMap.put(tablePath, writer);
            }

            List<FlussRowWithOp> rowWithOps = flussEvent.getRowWithOps();
            if (rowWithOps == null) {
                return;
            }
            for (FlussRowWithOp rowWithOp : rowWithOps) {
                FlussOperationType opType = rowWithOp.getOperationType();
                InternalRow row = rowWithOp.getRow();
                if (opType == FlussOperationType.IGNORE) {
                    // skip writing the row
                    return;
                }
                CompletableFuture<?> writeFuture =
                        write(writerMap.get(tablePath), opType, row, tablePath);
                writeFuture.whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                if (this.asyncWriterException == null) {
                                    this.asyncWriterException = throwable;
                                }

                                // Checking for exceptions from previous writes
                                mailboxExecutor.execute(
                                        this::checkAsyncException, "Update error metric");
                            }
                        });

                numRecordsOutCounter.inc();
            }

        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private CompletableFuture<?> write(
            TableWriter writer, FlussOperationType opType, InternalRow row, TablePath tablePath)
            throws IOException {
        if (writer instanceof UpsertWriter) {
            UpsertWriter upsertWriter = (UpsertWriter) writer;
            if (opType == FlussOperationType.UPSERT) {
                return upsertWriter.upsert(row);
            } else if (opType == FlussOperationType.DELETE) {
                return upsertWriter.delete(row);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported operation type: %s for primary key table %s",
                                opType, tablePath));
            }
        } else if (writer instanceof AppendWriter) {
            AppendWriter appendWriter = (AppendWriter) writer;
            if (opType == FlussOperationType.APPEND) {
                return appendWriter.append(row);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported operation type: %s for log table %s",
                                opType, tablePath));
            }
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported writer type: %s for table %s",
                            writer.getClass(), tablePath));
        }
    }

    public void flush(boolean endOfInput) throws IOException {
        for (TableWriter writer : writerMap.values()) {
            writer.flush();
            checkAsyncException();
        }
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Fluss sink function.");
        try {
            for (Table table : tableMap.values()) {
                table.close();
            }

            tableMap.clear();

            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            LOG.warn("Exception occurs while closing Fluss Connection.", e);
        }
        connection = null;

        if (flinkMetricRegistry != null) {
            flinkMetricRegistry.close();
        }
        flinkMetricRegistry = null;

        // Rethrow exception for the case in which close is called before writer() and flush().
        checkAsyncException();

        LOG.info("Finished closing Fluss sink function.");
    }

    private long computeSendTime() {
        if (flinkMetricRegistry == null) {
            return -1;
        }

        Metric writerSendLatencyMs =
                flinkMetricRegistry.getFlussMetric(MetricNames.WRITER_SEND_LATENCY_MS);
        if (writerSendLatencyMs == null) {
            return -1;
        }

        return ((Gauge<Long>) writerSendLatencyMs).getValue();
    }

    /**
     * This method should only be invoked in the mailbox thread since the counter is not volatile.
     * Logic needs to be invoked by write AND flush since we support various semantics.
     */
    protected void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Throwable throwable = asyncWriterException;
        if (throwable != null) {
            asyncWriterException = null;
            numRecordsOutErrorsCounter.inc();
            LOG.error("Exception occurs while write row to fluss.", throwable);
            throw new IOException(
                    "One or more Fluss Writer send requests have encountered exception", throwable);
        }
    }
}
