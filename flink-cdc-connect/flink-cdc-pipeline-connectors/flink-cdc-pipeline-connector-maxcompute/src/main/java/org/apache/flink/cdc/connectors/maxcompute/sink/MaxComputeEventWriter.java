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

package org.apache.flink.cdc.connectors.maxcompute.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.OperationType;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.SessionManageOperator;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CommitSessionRequest;
import org.apache.flink.cdc.connectors.maxcompute.coordinator.message.CommitSessionResponse;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeExecutionOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;
import org.apache.flink.cdc.connectors.maxcompute.writer.MaxComputeUpsertWriter;
import org.apache.flink.cdc.connectors.maxcompute.writer.MaxComputeWriter;

import com.aliyun.odps.tunnel.impl.UpsertRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** a {@link SinkWriter} for {@link Event} for MaxCompute. */
public class MaxComputeEventWriter implements SinkWriter<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeEventWriter.class);

    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final MaxComputeExecutionOptions executionOptions;
    private final Map<String, MaxComputeWriter> writerMap;
    private final Map<TableId, Schema> schemaCache;

    public MaxComputeEventWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            MaxComputeExecutionOptions executionOptions) {
        this.options = options;
        this.writeOptions = writeOptions;
        this.executionOptions = executionOptions;

        this.writerMap = new HashMap<>();
        this.schemaCache = new HashMap<>();
    }

    @Override
    public void write(Event element, Context context) throws IOException, InterruptedException {
        if (element instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) element;
            LOG.info("get dataChangeEvent {}", dataChangeEvent);
            String sessionId = dataChangeEvent.meta().get(Constant.TUNNEL_SESSION_ID);
            String partitionName = dataChangeEvent.meta().get(Constant.MAXCOMPUTE_PARTITION_NAME);
            if (!writerMap.containsKey(sessionId)) {
                writerMap.put(
                        sessionId,
                        new MaxComputeUpsertWriter(
                                options,
                                writeOptions,
                                executionOptions,
                                SessionIdentifier.of(
                                        options.getProject(),
                                        dataChangeEvent.tableId().getNamespace(),
                                        dataChangeEvent.tableId().getTableName(),
                                        partitionName,
                                        sessionId)));
            }
            MaxComputeWriter writer = writerMap.get(sessionId);
            UpsertRecord record = writer.newElement();

            if (dataChangeEvent.op() != OperationType.DELETE) {
                TypeConvertUtils.toMaxComputeRecord(
                        schemaCache.get(dataChangeEvent.tableId()),
                        dataChangeEvent.after(),
                        record);
                writer.write(record);
            } else {
                TypeConvertUtils.toMaxComputeRecord(
                        schemaCache.get(dataChangeEvent.tableId()),
                        dataChangeEvent.before(),
                        record);
                writer.delete(record);
            }
        } else if (element instanceof CreateTableEvent) {
            CreateTableEvent createTableEvent = (CreateTableEvent) element;
            schemaCache.put(createTableEvent.tableId(), createTableEvent.getSchema());
        } else if (element instanceof SchemaChangeEvent) {
            SchemaChangeEvent schemaChangeEvent = (SchemaChangeEvent) element;
            TableId tableId = schemaChangeEvent.tableId();
            Schema newSchema =
                    SchemaUtils.applySchemaChangeEvent(schemaCache.get(tableId), schemaChangeEvent);
            schemaCache.put(tableId, newSchema);
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        SessionManageOperator operator = SessionManageOperator.instance;
        LOG.info("Sink writer {} start to flush.", operator.getOperatorIndex());
        writerMap.forEach(
                (sessionId, writer) -> {
                    try {
                        writer.flush();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        writerMap.clear();
        try {
            CommitSessionResponse response =
                    (CommitSessionResponse)
                            operator.sendRequestToOperator(
                                    new CommitSessionRequest(operator.getOperatorIndex()));
            if (!response.isSuccess()) {
                throw new IOException("JobManager commit session failed. restart all TaskManager");
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
        LOG.info("Sink writer {} flush success.", operator.getOperatorIndex());
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
