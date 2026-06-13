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

package org.apache.flink.cdc.connectors.lancedb.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.lancedb.client.DefaultLanceDbClient;
import org.apache.flink.cdc.connectors.lancedb.client.LanceDbClient;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbPathUtils;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbTypeUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_TABLE_COMMENT;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;

/** Applies schema changes to Lance datasets. */
public class LanceDbMetadataApplier implements MetadataApplier {

    private final LanceDbDataSinkConfig config;
    private final LanceDbClient client;
    private final Map<String, TableId> datasetOwners;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;

    public LanceDbMetadataApplier(LanceDbDataSinkConfig config) {
        this(config, new DefaultLanceDbClient(config));
    }

    LanceDbMetadataApplier(LanceDbDataSinkConfig config, LanceDbClient client) {
        this.config = config;
        this.client = client;
        this.datasetOwners = new HashMap<>();
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
    }

    @Override
    public MetadataApplier setAcceptedSchemaEvolutionTypes(
            Set<SchemaChangeEventType> schemaEvolutionTypes) {
        this.enabledSchemaEvolutionTypes = schemaEvolutionTypes;
        return this;
    }

    @Override
    public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
        return enabledSchemaEvolutionTypes.contains(schemaChangeEventType);
    }

    @Override
    public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
        if (config.isSchemaEvolutionEnabled()) {
            return Sets.newHashSet(CREATE_TABLE, ADD_COLUMN, ALTER_TABLE_COMMENT);
        }
        return Sets.newHashSet(CREATE_TABLE, ALTER_TABLE_COMMENT);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
            throw new SchemaEvolveException(
                    schemaChangeEvent,
                    "LanceDB sink does not support schema evolution type "
                            + schemaChangeEvent.getType()
                            + " with current configuration.",
                    null);
        }
        try {
            SchemaChangeEventVisitor.<Void, Exception>visit(
                    schemaChangeEvent,
                    addColumnEvent -> {
                        applyAddColumn(addColumnEvent);
                        return null;
                    },
                    alterColumnTypeEvent -> reject(alterColumnTypeEvent),
                    createTableEvent -> {
                        applyCreateTable(createTableEvent);
                        return null;
                    },
                    dropColumnEvent -> reject(dropColumnEvent),
                    dropTableEvent -> reject(dropTableEvent),
                    renameColumnEvent -> reject(renameColumnEvent),
                    truncateTableEvent -> reject(truncateTableEvent),
                    alterTableCommentEvent -> null);
        } catch (Exception e) {
            if (e instanceof SchemaEvolveException) {
                throw (SchemaEvolveException) e;
            }
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    private void applyCreateTable(CreateTableEvent event) {
        String datasetPath = LanceDbPathUtils.resolveDatasetPath(event.tableId(), config);
        registerDatasetOwner(datasetPath, event.tableId());
        org.apache.arrow.vector.types.pojo.Schema expected =
                LanceDbTypeUtils.toArrowSchema(event.getSchema(), isAppendWithMetadataMode());
        if (!client.datasetExists(datasetPath)) {
            if (!config.isCreateTableEnabled()) {
                throw new IllegalStateException(
                        "Lance dataset "
                                + datasetPath
                                + " does not exist and create-table.enabled is false.");
            }
            client.createDataset(datasetPath, expected);
            return;
        }
        if (config.isSchemaValidationEnabled()) {
            LanceDbTypeUtils.validateCompatible(
                    datasetPath, expected, client.getSchema(datasetPath));
        }
    }

    private void applyAddColumn(AddColumnEvent event) {
        if (!config.isSchemaEvolutionEnabled()) {
            throw new IllegalStateException(
                    "LanceDB ADD_COLUMN requires schema.evolution.enabled=true.");
        }
        String datasetPath = LanceDbPathUtils.resolveDatasetPath(event.tableId(), config);
        registerDatasetOwner(datasetPath, event.tableId());
        org.apache.arrow.vector.types.pojo.Schema existing = client.getSchema(datasetPath);
        List<Field> missingFields = new ArrayList<>();
        for (AddColumnEvent.ColumnWithPosition columnWithPosition : event.getAddedColumns()) {
            if (columnWithPosition.getPosition() != AddColumnEvent.ColumnPosition.LAST) {
                throw new IllegalStateException(
                        "LanceDB sink only supports ADD_COLUMN at LAST position.");
            }
            Column column = columnWithPosition.getAddColumn();
            Field expected =
                    LanceDbTypeUtils.toArrowField(
                            column.getName(), column.getType(), column.getType().isNullable());
            Field actual = findField(existing, column.getName());
            if (actual == null) {
                missingFields.add(expected);
                continue;
            }
            LanceDbTypeUtils.validateCompatibleField(
                    datasetPath, expected, actual, column.getName());
        }
        if (!missingFields.isEmpty()) {
            client.addColumns(datasetPath, missingFields);
        }
    }

    private Void reject(SchemaChangeEvent event) {
        throw new UnsupportedOperationException(
                "LanceDB sink does not support schema evolution type "
                        + event.getType()
                        + " for table "
                        + event.tableId()
                        + ".");
    }

    private Field findField(org.apache.arrow.vector.types.pojo.Schema schema, String fieldName) {
        for (Field field : schema.getFields()) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    private void registerDatasetOwner(String datasetPath, TableId tableId) {
        TableId existing = datasetOwners.putIfAbsent(datasetPath, tableId);
        if (existing != null && !existing.equals(tableId)) {
            throw new IllegalStateException(
                    "Lance dataset path "
                            + datasetPath
                            + " is already owned by source table "
                            + existing
                            + " and cannot also be used by "
                            + tableId
                            + ". Configure table.path.mapping to unique dataset paths.");
        }
    }

    private boolean isAppendWithMetadataMode() {
        return "append-with-metadata".equals(config.getChangelogMode());
    }
}
