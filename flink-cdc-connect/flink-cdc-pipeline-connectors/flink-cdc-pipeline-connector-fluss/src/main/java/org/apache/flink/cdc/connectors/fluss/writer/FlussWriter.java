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

package org.apache.flink.cdc.connectors.fluss.writer;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.table.writer.TableWriter;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.row.RowWithOp;
import com.alibaba.fluss.flink.sink.serializer.FlussSerializationSchema;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FlussWriter implements SinkWriter<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(FlussWriter.class);

    private final Sink.InitContext context;
    private final Configuration config;
    private final Map<TableId, UpsertWriter> writerMap;
    private final Map<TableId, Schema> schemaCache;
    private transient Connection connection;
    private volatile Throwable asyncWriterException;
    private final FlussSerializationSchema<Event> serializationSchema;

    public FlussWriter(
            Sink.InitContext context,
            Configuration config,
            FlussSerializationSchema<Event> serializationSchema) {
        this.context = context;
        this.config = config;
        this.writerMap = new HashMap<>();
        this.schemaCache = new HashMap<>();
        this.serializationSchema = serializationSchema;
        try {
            this.serializationSchema.open(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void initialize() {
        LOG.info(
                "Opening Fluss {} with configuration: {}", this.getClass().getSimpleName(), config);
        connection = ConnectionFactory.createConnection(config);
    }

    @Override
    public void write(Event element, Context context) throws IOException, InterruptedException {
        try {
            RowWithOp rowWithOp = serializationSchema.serialize(element);
            if (rowWithOp == null) {
                return;
            }
            UpsertWriter writer =
                    writerMap.computeIfAbsent(
                            ((DataChangeEvent) element).tableId(),
                            tableId ->
                                    connection
                                            .getTable(
                                                    TablePath.of(
                                                            tableId.getSchemaName(),
                                                            tableId.getTableName()))
                                            .newUpsert()
                                            .createWriter());
            CompletableFuture<?> writeFuture =
                    writeRow(rowWithOp.getOperationType(), writer, rowWithOp.getRow());
            writeFuture.whenComplete(
                    (ignored, throwable) -> {
                        if (throwable != null) {
                            if (this.asyncWriterException == null) {
                                this.asyncWriterException = throwable;
                            }

                            // Checking for exceptions from previous writes
                            this.context
                                    .getMailboxExecutor()
                                    .execute(this::checkAsyncException, "Check async exception");
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    CompletableFuture<?> writeRow(
            com.alibaba.fluss.flink.row.OperationType opType,
            UpsertWriter upsertWriter,
            InternalRow internalRow) {
        if (opType == com.alibaba.fluss.flink.row.OperationType.UPSERT) {
            return upsertWriter.upsert(internalRow);
        } else if (opType == com.alibaba.fluss.flink.row.OperationType.DELETE) {
            return upsertWriter.delete(internalRow);
        } else {
            throw new UnsupportedOperationException("Unsupported operation type: " + opType);
        }
    }

    private void checkAsyncException() throws IOException {
        // reset this exception since we could close the writer later on
        Throwable throwable = asyncWriterException;
        if (throwable != null) {
            asyncWriterException = null;
            LOG.error("Exception occurs while write row to fluss.", throwable);
            throw new IOException(
                    "One or more Fluss Writer send requests have encountered exception", throwable);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        writerMap.values().forEach(TableWriter::flush);
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
