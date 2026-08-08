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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.table.writer.MultiTableWriteRecord;
import org.apache.fluss.client.table.writer.MultiTableWriter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.Gauge;
import org.apache.fluss.metrics.Metric;
import org.apache.fluss.metrics.MetricNames;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
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

    private transient MultiTableWriter multiTableWriter;

    public FlussSinkWriter(
            Configuration flussConfig,
            MailboxExecutor mailboxExecutor,
            FlussEventSerializer<InputT> flussRecordSerializer) {
        this.flussConfig = flussConfig;
        this.mailboxExecutor = mailboxExecutor;
        this.flussRecordSerializer = flussRecordSerializer;
    }

    public void initialize(SinkWriterMetricGroup metricGroup) throws IOException {
        LOG.info("Opening Fluss with config {}", flussConfig);
        this.metricGroup = metricGroup;
        flinkMetricRegistry =
                new WrapperFlussMetricRegistry(
                        metricGroup, Collections.singleton(MetricNames.WRITER_SEND_LATENCY_MS));
        connection = ConnectionFactory.createConnection(flussConfig, flinkMetricRegistry);
        flussRecordSerializer.open(connection);
        multiTableWriter = connection.getMultiTable().newMultiTableWrite().createWriter();

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
            if (flussEvent == null || flussEvent.getRowWithOps() == null) {
                return;
            }

            TablePath tablePath = flussEvent.getTablePath();
            int schemaId = flussEvent.getSchemaId();

            for (FlussRowWithOp rowWithOp : flussEvent.getRowWithOps()) {
                FlussOperationType opType = rowWithOp.getOperationType();
                InternalRow row = rowWithOp.getRow();
                if (opType == FlussOperationType.IGNORE) {
                    // skip writing the row
                    continue;
                }
                MultiTableWriteRecord writeRecord = toWriteRecord(opType, tablePath, row, schemaId);
                LOG.info("------writeRecord  " + writeRecord);
                CompletableFuture<?> writeFuture = multiTableWriter.write(writeRecord);
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

    private static MultiTableWriteRecord toWriteRecord(
            FlussOperationType opType, TablePath tablePath, InternalRow row, int schemaId) {
        switch (opType) {
            case APPEND:
                return MultiTableWriteRecord.forAppend(tablePath, row, schemaId);
            case UPSERT:
                return MultiTableWriteRecord.forUpsert(tablePath, row, schemaId);
            case DELETE:
                return MultiTableWriteRecord.forDelete(tablePath, row, schemaId);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported operation type: %s for table %s", opType, tablePath));
        }
    }

    public void flush(boolean endOfInput) throws IOException {
        if (multiTableWriter != null) {
            multiTableWriter.flush();
        }
        checkAsyncException();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing Fluss sink function.");
        try {
            if (multiTableWriter != null) {
                // close() flushes pending records first.
                multiTableWriter.close();
                multiTableWriter = null;
            }

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
