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

package org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.WriteResultWrapper;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.actions.Actions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** An Operator to trigger file compaction conditionally. */
public class CompactionOperator
        extends AbstractStreamOperator<CommittableMessage<WriteResultWrapper>>
        implements OneInputStreamOperator<
                CommittableMessage<WriteResultWrapper>, CommittableMessage<WriteResultWrapper>> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(CompactionOperator.class);

    private final Map<String, String> catalogOptions;

    private Catalog catalog;

    /** store a list of MultiTableCommittable in one checkpoint. */
    private final Map<TableId, Integer> tableCommitTimes;

    private final Set<TableId> compactedTables;

    private final CompactionOptions compactionOptions;

    private volatile Throwable throwable;

    private ExecutorService compactExecutor;

    public CompactionOperator(
            Map<String, String> catalogOptions, CompactionOptions compactionOptions) {
        this.tableCommitTimes = new HashMap<>();
        this.compactedTables = new HashSet<>();
        this.catalogOptions = catalogOptions;
        this.compactionOptions = compactionOptions;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (compactExecutor == null) {
            this.compactExecutor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ExecutorThreadFactory(
                                    Thread.currentThread().getName() + "-Cdc-Compaction"));
        }
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<WriteResultWrapper>> element) {
        if (element.getValue() instanceof CommittableWithLineage) {
            TableId tableId =
                    ((CommittableWithLineage<WriteResultWrapper>) element.getValue())
                            .getCommittable()
                            .getTableId();
            tableCommitTimes.put(tableId, tableCommitTimes.getOrDefault(tableId, 0) + 1);
            int commitTimes = tableCommitTimes.get(tableId);
            if (commitTimes >= compactionOptions.getCommitInterval()
                    && !compactedTables.contains(tableId)) {
                if (throwable != null) {
                    throw new RuntimeException(throwable);
                }
                compactedTables.add(tableId);
                if (compactExecutor == null) {
                    compact(tableId);
                } else {
                    compactExecutor.submit(() -> compact(tableId));
                }
            }
        }
    }

    private void compact(TableId tableId) {
        if (catalog == null) {
            catalog =
                    CatalogUtil.buildIcebergCatalog(
                            this.getClass().getSimpleName(), catalogOptions, new Configuration());
        }
        try {
            RewriteDataFilesActionResult rewriteResult =
                    Actions.forTable(
                                    StreamExecutionEnvironment.createLocalEnvironment(),
                                    catalog.loadTable(TableIdentifier.parse(tableId.identifier())))
                            .rewriteDataFiles()
                            .execute();
            LOGGER.info(
                    "Iceberg small file compact result for {}: added {} data files and deleted {} files.",
                    tableId.identifier(),
                    rewriteResult.addedDataFiles().size(),
                    rewriteResult.deletedDataFiles().size());
        } catch (Throwable t) {
            throwable = t;
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) {
        // Reset the commit times.
        for (TableId tableId : compactedTables) {
            tableCommitTimes.put(tableId, 0);
        }
        compactedTables.clear();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (compactExecutor != null) {
            compactExecutor.shutdown();
        }
        catalog = null;
    }
}
