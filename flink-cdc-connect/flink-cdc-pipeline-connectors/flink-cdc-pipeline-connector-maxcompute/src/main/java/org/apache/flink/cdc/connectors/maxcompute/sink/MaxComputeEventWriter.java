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

import org.apache.flink.api.connector.sink2.Sink;
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
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;
import org.apache.flink.cdc.connectors.maxcompute.writer.MaxComputeWriter;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.Preconditions;

import com.aliyun.odps.data.ArrayRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** a {@link SinkWriter} for {@link Event} for MaxCompute. */
public class MaxComputeEventWriter implements SinkWriter<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeEventWriter.class);

    private final Sink.InitContext context;
    private final MaxComputeOptions options;
    private final MaxComputeWriteOptions writeOptions;
    private final Map<String, MaxComputeWriter> writerMap;
    private final Map<TableId, Schema> schemaCache;

    public MaxComputeEventWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            Sink.InitContext context) {
        this.context = context;
        this.options = options;
        this.writeOptions = writeOptions;

        this.writerMap = new HashMap<>();
        this.schemaCache = new HashMap<>();
    }

    @Override
    public void write(Event element, Context context) throws IOException {
        if (element instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) element;
            String sessionId = dataChangeEvent.meta().get(Constant.TUNNEL_SESSION_ID);
            String partitionName = dataChangeEvent.meta().get(Constant.MAXCOMPUTE_PARTITION_NAME);
            if (!writerMap.containsKey(sessionId)) {
                LOG.info(
                        "Sink writer {} start to create session {}.",
                        this.context.getSubtaskId(),
                        sessionId);
                SessionIdentifier sessionIdentifier =
                        SessionIdentifier.of(
                                options.getProject(),
                                MaxComputeUtils.getSchema(options, dataChangeEvent.tableId()),
                                dataChangeEvent.tableId().getTableName(),
                                partitionName,
                                sessionId);
                writerMap.put(
                        sessionId,
                        MaxComputeWriter.batchWriter(options, writeOptions, sessionIdentifier));
            }
            MaxComputeWriter writer = writerMap.get(sessionId);
            ArrayRecord record = writer.newElement();

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
        Preconditions.checkNotNull(
                operator,
                "SessionManageOperator cannot be null, please setting 'pipeline.operator-chaining' to true to avoid this issue.");
        LOG.info("Sink writer {} start to flush.", context.getSubtaskId());
        List<Future<CoordinationResponse>> responces = new ArrayList<>(writerMap.size() + 1);
        writerMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(
                        entry -> {
                            try {
                                entry.getValue().flush();
                                Future<CoordinationResponse> future =
                                        operator.submitRequestToOperator(
                                                new CommitSessionRequest(
                                                        context.getSubtaskId(), entry.getKey()));
                                responces.add(future);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        writerMap.clear();
        Future<CoordinationResponse> future =
                operator.submitRequestToOperator(
                        new CommitSessionRequest(context.getSubtaskId(), Constant.END_OF_SESSION));
        responces.add(future);
        try {
            for (Future<CoordinationResponse> response : responces) {
                CommitSessionResponse commitSessionResponse =
                        CoordinationResponseUtils.unwrap(response.get());
                if (!commitSessionResponse.isSuccess()) {
                    throw new IOException(
                            "JobManager commit session failed. restart all TaskManager");
                }
            }
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
        LOG.info("Sink writer {} flush success.", context.getSubtaskId());
    }

    @Override
    public void close() throws Exception {
        // do nothing
    }
}
