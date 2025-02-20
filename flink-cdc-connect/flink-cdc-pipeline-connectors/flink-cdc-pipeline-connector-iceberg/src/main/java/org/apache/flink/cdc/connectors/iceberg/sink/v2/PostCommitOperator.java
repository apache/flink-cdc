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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.actions.Actions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** An Operator to add checkpointId to IcebergTable Small file compaction. */
public class PostCommitOperator
        extends AbstractStreamOperator<CommittableMessage<WriteResultWrapper>>
        implements OneInputStreamOperator<
                CommittableMessage<WriteResultWrapper>, CommittableMessage<WriteResultWrapper>> {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PostCommitOperator.class);

    private final Map<String, String> catalogOptions;

    private Catalog catalog;

    /** store a list of MultiTableCommittable in one checkpoint. */
    private final List<WriteResultWrapper> multiTableCommittables;

    public PostCommitOperator(Map<String, String> catalogOptions) {
        multiTableCommittables = new ArrayList<>();
        this.catalogOptions = catalogOptions;
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<WriteResultWrapper>> element) {
        if (element.getValue() instanceof CommittableWithLineage) {
            multiTableCommittables.add(
                    ((CommittableWithLineage<WriteResultWrapper>) element.getValue())
                            .getCommittable());
        }
    }

    @Override
    public void finish() {
        prepareSnapshotPreBarrier(Long.MAX_VALUE);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        if (catalog == null) {
            this.catalog =
                    CatalogUtil.buildIcebergCatalog(
                            "cdc-iceberg-catalog", catalogOptions, new Configuration());
        }
        for (int i = 0; i < multiTableCommittables.size(); i++) {
            WriteResultWrapper multiTableCommittable = multiTableCommittables.get(i);
            multiTableCommittables.set(
                    i,
                    new WriteResultWrapper(
                            multiTableCommittable.getWriteResult(),
                            multiTableCommittable.getTableId()));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        long checkpointId = context.getCheckpointId();
        if (!multiTableCommittables.isEmpty()) {
            multiTableCommittables.forEach(
                    (multiTableCommittable) -> {
                        LOGGER.debug(
                                "Try to commit for {}.{} : {} in checkpoint {}",
                                multiTableCommittable.getWriteResult(),
                                multiTableCommittable.getTableId(),
                                multiTableCommittables,
                                checkpointId);
                        String tableName = multiTableCommittable.getTableId().getTableName();
                        RewriteDataFilesActionResult rewriteResult =
                                Actions.forTable(catalog.loadTable(TableIdentifier.of(tableName)))
                                        .rewriteDataFiles()
                                        .targetSizeInBytes(128 * 1024 * 1024)
                                        .execute();
                        LOGGER.info(
                                "Iceberg small file compact for table {} flushed {} data files and {} delete files",
                                tableName,
                                rewriteResult.addedDataFiles().size(),
                                rewriteResult.deletedDataFiles().size());
                    });
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
