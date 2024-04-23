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

package org.apache.flink.cdc.connectors.maxcompute;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.maxcompute.common.UncheckedOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.SchemaEvolutionUtils;
import org.apache.flink.cdc.connectors.maxcompute.utils.TypeConvertUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link MetadataApplier} for "MaxCompute" connector. */
public class MaxComputeMetadataApplier implements MetadataApplier {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeMetadataApplier.class);

    private final MaxComputeOptions maxComputeOptions;

    public MaxComputeMetadataApplier(MaxComputeOptions maxComputeOptions) {
        this.maxComputeOptions = maxComputeOptions;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {
        LOG.info("MaxCompute apply schema change event: {}", schemaChangeEvent);
        try {
            if (schemaChangeEvent instanceof CreateTableEvent) {
                CreateTableEvent createTableEvent = (CreateTableEvent) schemaChangeEvent;
                if (MaxComputeUtils.isTableExist(maxComputeOptions, createTableEvent.tableId())) {
                    TableSchema currentSchema =
                            MaxComputeUtils.getTableSchema(
                                    maxComputeOptions, createTableEvent.tableId());
                    TableSchema expectSchema =
                            TypeConvertUtils.toMaxCompute(createTableEvent.getSchema());
                    if (!MaxComputeUtils.schemaEquals(currentSchema, expectSchema)) {
                        throw new IllegalStateException(
                                "The schema of create table event is not equals to exist table schema, please drop/rename exist table before flink cdc task start.");
                    }
                } else {
                    SchemaEvolutionUtils.createTable(
                            maxComputeOptions,
                            createTableEvent.tableId(),
                            createTableEvent.getSchema());
                }
            } else if (schemaChangeEvent instanceof AlterColumnTypeEvent) {
                AlterColumnTypeEvent alterColumnTypeEvent =
                        (AlterColumnTypeEvent) schemaChangeEvent;
                SchemaEvolutionUtils.alterColumnType(
                        maxComputeOptions,
                        alterColumnTypeEvent.tableId(),
                        alterColumnTypeEvent.getTypeMapping());
            } else if (schemaChangeEvent instanceof DropColumnEvent) {
                DropColumnEvent dropColumnEvent = (DropColumnEvent) schemaChangeEvent;
                SchemaEvolutionUtils.dropColumn(
                        maxComputeOptions,
                        dropColumnEvent.tableId(),
                        dropColumnEvent.getDroppedColumnNames());
            } else if (schemaChangeEvent instanceof RenameColumnEvent) {
                RenameColumnEvent renameColumnEvent = (RenameColumnEvent) schemaChangeEvent;
                SchemaEvolutionUtils.renameColumn(
                        maxComputeOptions,
                        renameColumnEvent.tableId(),
                        renameColumnEvent.getNameMapping());
            } else if (schemaChangeEvent instanceof AddColumnEvent) {
                AddColumnEvent addColumnEvent = (AddColumnEvent) schemaChangeEvent;
                SchemaEvolutionUtils.addColumns(
                        maxComputeOptions,
                        addColumnEvent.tableId(),
                        addColumnEvent.getAddedColumns());
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported schema change event: "
                                + schemaChangeEvent.getClass().getName());
            }
        } catch (OdpsException e) {
            throw new UncheckedOdpsException(e);
        }
    }
}
