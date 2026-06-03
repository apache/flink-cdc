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

package org.apache.flink.cdc.connectors.milvus.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.AlterTableCommentEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.event.TruncateTableEvent;
import org.apache.flink.cdc.common.event.visitor.SchemaChangeEventVisitor;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusClientUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusCollectionUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusNameUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusRetryUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusTypeUtils;
import org.apache.flink.cdc.connectors.milvus.utils.MilvusVectorFieldSpec;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddCollectionFieldReq;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.HasPartitionReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ADD_COLUMN;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.ALTER_TABLE_COMMENT;
import static org.apache.flink.cdc.common.event.SchemaChangeEventType.CREATE_TABLE;

/** Applies schema changes to Milvus collections. */
public class MilvusMetadataApplier implements MetadataApplier {

    private static final Logger LOG = LoggerFactory.getLogger(MilvusMetadataApplier.class);

    private final MilvusDataSinkConfig config;
    private Set<SchemaChangeEventType> enabledSchemaEvolutionTypes;
    private final Map<String, TableId> collectionOwners;

    public MilvusMetadataApplier(MilvusDataSinkConfig config) {
        this.config = config;
        this.enabledSchemaEvolutionTypes = getSupportedSchemaEvolutionTypes();
        this.collectionOwners = new HashMap<>();
    }

    void registerCollectionNameForTest(TableId tableId) {
        MilvusCollectionUtils.registerCollectionName(tableId, config, collectionOwners);
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
        return Sets.newHashSet(CREATE_TABLE, ADD_COLUMN, ALTER_TABLE_COMMENT);
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
            throws SchemaEvolveException {
        if (!acceptsSchemaEvolutionType(schemaChangeEvent.getType())) {
            throw new SchemaEvolveException(
                    schemaChangeEvent,
                    "Milvus sink does not support schema evolution type "
                            + schemaChangeEvent.getType()
                            + ".",
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
                    alterTableCommentEvent -> {
                        applyAlterTableComment(alterTableCommentEvent);
                        return null;
                    });
        } catch (Exception e) {
            if (e instanceof SchemaEvolveException) {
                throw (SchemaEvolveException) e;
            }
            throw new SchemaEvolveException(schemaChangeEvent, e.getMessage(), e);
        }
    }

    private void applyCreateTable(CreateTableEvent event) {
        MilvusCollectionUtils.registerCollectionName(event.tableId(), config, collectionOwners);
        withRetry(
                () -> {
                    MilvusClientV2 client = MilvusClientUtils.createClient(config);
                    try {
                        String collectionName =
                                MilvusCollectionUtils.resolveCollectionName(
                                        event.tableId(), config);
                        boolean exists =
                                client.hasCollection(
                                        HasCollectionReq.builder()
                                                .databaseName(config.getDatabaseName())
                                                .collectionName(collectionName)
                                                .build());
                        if (!exists) {
                            if (!config.isCreateCollectionEnabled()) {
                                throw new IllegalStateException(
                                        "Milvus collection "
                                                + collectionName
                                                + " does not exist and create-collection.enabled is false.");
                            }
                            client.createCollection(
                                    MilvusCollectionUtils.buildCreateCollectionReq(
                                            event.tableId(), event.getSchema(), config));
                        } else {
                            DescribeCollectionResp existing =
                                    client.describeCollection(
                                            DescribeCollectionReq.builder()
                                                    .databaseName(config.getDatabaseName())
                                                    .collectionName(collectionName)
                                                    .build());
                            MilvusCollectionUtils.validateExistingCollection(
                                    event.tableId(), event.getSchema(), existing, config);
                            createMissingIndexesIfNeeded(client, collectionName, event.tableId());
                        }
                        createConfiguredPartitionsIfNeeded(client, collectionName);
                        loadCollectionIfNeeded(client, collectionName);
                    } finally {
                        client.close();
                    }
                    return null;
                },
                "create table " + event.tableId());
    }

    private void applyAddColumn(AddColumnEvent event) {
        withRetry(
                () -> {
                    String collectionName =
                            MilvusCollectionUtils.resolveCollectionName(event.tableId(), config);
                    MilvusClientV2 client = MilvusClientUtils.createClient(config);
                    try {
                        for (AddColumnEvent.ColumnWithPosition columnWithPosition :
                                event.getAddedColumns()) {
                            Column column = columnWithPosition.getAddColumn();
                            if (MilvusTypeUtils.findVectorFieldSpec(
                                            column.getName(),
                                            config.getVectorFields(event.tableId()))
                                    .isPresent()) {
                                throw new IllegalStateException(
                                        "Milvus sink does not support adding vector field "
                                                + column.getName()
                                                + ".");
                            }
                            AddFieldReq addFieldReq =
                                    MilvusCollectionUtils.buildAddScalarFieldReq(column, config);
                            if (addFieldReq.getDataType() == DataType.FloatVector) {
                                throw new IllegalStateException(
                                        "Milvus sink does not support adding vector field "
                                                + column.getName()
                                                + ".");
                            }
                            DescribeCollectionResp collection =
                                    client.describeCollection(
                                            DescribeCollectionReq.builder()
                                                    .databaseName(config.getDatabaseName())
                                                    .collectionName(collectionName)
                                                    .build());
                            if (isAddFieldAlreadyApplied(
                                    collection, addFieldReq, collectionName, column.getName())) {
                                continue;
                            }
                            try {
                                client.addCollectionField(
                                        AddCollectionFieldReq.builder()
                                                .databaseName(config.getDatabaseName())
                                                .collectionName(collectionName)
                                                .fieldName(addFieldReq.getFieldName())
                                                .dataType(addFieldReq.getDataType())
                                                .maxLength(addFieldReq.getMaxLength())
                                                .isNullable(addFieldReq.getIsNullable())
                                                .defaultValue(addFieldReq.getDefaultValue())
                                                .enableDefaultValue(
                                                        addFieldReq.isEnableDefaultValue())
                                                .build());
                            } catch (RuntimeException e) {
                                if (!MilvusRetryUtils.isFieldAlreadyExistsError(e)) {
                                    throw e;
                                }
                                DescribeCollectionResp refreshedCollection =
                                        client.describeCollection(
                                                DescribeCollectionReq.builder()
                                                        .databaseName(config.getDatabaseName())
                                                        .collectionName(collectionName)
                                                        .build());
                                if (!isAddFieldAlreadyApplied(
                                        refreshedCollection,
                                        addFieldReq,
                                        collectionName,
                                        column.getName())) {
                                    throw e;
                                }
                            }
                        }
                    } finally {
                        client.close();
                    }
                    return null;
                },
                "add column to " + event.tableId());
    }

    private boolean isAddFieldAlreadyApplied(
            DescribeCollectionResp collection,
            AddFieldReq expectedField,
            String collectionName,
            String sourceColumnName) {
        if (collection.getCollectionSchema() == null) {
            return false;
        }
        CreateCollectionReq.FieldSchema existingField =
                collection.getCollectionSchema().getField(expectedField.getFieldName());
        if (existingField == null) {
            return false;
        }
        validateExistingAddedField(existingField, expectedField, collectionName, sourceColumnName);
        LOG.info(
                "Milvus field {} already exists on collection {}. Skip add-column replay.",
                expectedField.getFieldName(),
                collectionName);
        return true;
    }

    private void validateExistingAddedField(
            CreateCollectionReq.FieldSchema existingField,
            AddFieldReq expectedField,
            String collectionName,
            String sourceColumnName) {
        if (existingField.getDataType() != expectedField.getDataType()) {
            throw new IllegalStateException(
                    "Existing Milvus field "
                            + sourceColumnName
                            + " in collection "
                            + collectionName
                            + " type mismatch. Expected "
                            + expectedField.getDataType()
                            + " but was "
                            + existingField.getDataType()
                            + ".");
        }
        if (expectedField.getDataType() == DataType.VarChar
                && expectedField.getMaxLength() != null
                && existingField.getMaxLength() != null
                && existingField.getMaxLength() < expectedField.getMaxLength()) {
            throw new IllegalStateException(
                    "Existing Milvus field "
                            + sourceColumnName
                            + " in collection "
                            + collectionName
                            + " max length is too small. Expected at least "
                            + expectedField.getMaxLength()
                            + " but was "
                            + existingField.getMaxLength()
                            + ".");
        }
        if (Boolean.FALSE.equals(existingField.getIsNullable())
                && Boolean.TRUE.equals(expectedField.getIsNullable())) {
            throw new IllegalStateException(
                    "Existing Milvus field "
                            + sourceColumnName
                            + " in collection "
                            + collectionName
                            + " is NOT NULL, but the source column is nullable.");
        }
    }

    private void createMissingIndexesIfNeeded(
            MilvusClientV2 client, String collectionName, TableId tableId) {
        if (!config.isCreateIndexEnabled()) {
            return;
        }
        List<String> existingIndexes =
                client.listIndexes(
                        ListIndexesReq.builder()
                                .databaseName(config.getDatabaseName())
                                .collectionName(collectionName)
                                .build());
        List<MilvusVectorFieldSpec> vectorFields = config.getVectorFields(tableId);
        for (MilvusVectorFieldSpec vectorField : vectorFields) {
            String indexName = MilvusNameUtils.indexName(vectorField.getFieldName());
            if (existingIndexes.contains(indexName)) {
                continue;
            }
            try {
                client.createIndex(
                        CreateIndexReq.builder()
                                .databaseName(config.getDatabaseName())
                                .collectionName(collectionName)
                                .indexParams(
                                        MilvusCollectionUtils.buildIndexParams(
                                                Collections.singletonList(vectorField), config))
                                .sync(true)
                                .build());
            } catch (RuntimeException e) {
                if (MilvusRetryUtils.isIndexAlreadyExistsError(e)) {
                    LOG.info(
                            "Milvus index {} already exists on collection {}.",
                            indexName,
                            collectionName);
                    continue;
                }
                throw e;
            }
        }
    }

    private Void reject(AlterColumnTypeEvent event) throws SchemaEvolveException {
        throw unsupported(event);
    }

    private Void reject(DropColumnEvent event) throws SchemaEvolveException {
        throw unsupported(event);
    }

    private Void reject(DropTableEvent event) throws SchemaEvolveException {
        throw unsupported(event);
    }

    private Void reject(RenameColumnEvent event) throws SchemaEvolveException {
        throw unsupported(event);
    }

    private Void reject(TruncateTableEvent event) throws SchemaEvolveException {
        throw unsupported(event);
    }

    private void applyAlterTableComment(AlterTableCommentEvent event) {
        // Milvus collection comments are not evolved by the MVP connector.
    }

    private void loadCollectionIfNeeded(MilvusClientV2 client, String collectionName) {
        if (!config.isLoadCollectionEnabled()) {
            return;
        }
        boolean loaded =
                client.getLoadState(
                        GetLoadStateReq.builder()
                                .databaseName(config.getDatabaseName())
                                .collectionName(collectionName)
                                .build());
        if (!loaded) {
            client.loadCollection(
                    LoadCollectionReq.builder()
                            .databaseName(config.getDatabaseName())
                            .collectionName(collectionName)
                            .sync(true)
                            .timeout(config.getRpcDeadline().toMillis())
                            .build());
        }
    }

    private void createConfiguredPartitionsIfNeeded(MilvusClientV2 client, String collectionName) {
        if (config.getPartitionNames().isEmpty()) {
            return;
        }
        for (String partitionName : config.getPartitionNames()) {
            boolean exists =
                    client.hasPartition(
                            HasPartitionReq.builder()
                                    .databaseName(config.getDatabaseName())
                                    .collectionName(collectionName)
                                    .partitionName(partitionName)
                                    .build());
            if (!exists) {
                try {
                    client.createPartition(
                            CreatePartitionReq.builder()
                                    .databaseName(config.getDatabaseName())
                                    .collectionName(collectionName)
                                    .partitionName(partitionName)
                                    .build());
                } catch (RuntimeException e) {
                    if (MilvusRetryUtils.isPartitionAlreadyExistsError(e)
                            || client.hasPartition(
                                    HasPartitionReq.builder()
                                            .databaseName(config.getDatabaseName())
                                            .collectionName(collectionName)
                                            .partitionName(partitionName)
                                            .build())) {
                        continue;
                    }
                    throw e;
                }
            }
        }
    }

    private <T> T withRetry(RetryableOperation<T> operation, String action) {
        RuntimeException failure = null;
        for (int attempt = 0; attempt <= config.getMaxRetries(); attempt++) {
            try {
                return operation.run();
            } catch (RuntimeException e) {
                failure = e;
                if (!MilvusRetryUtils.isRetryable(e) || attempt >= config.getMaxRetries()) {
                    break;
                }
                LOG.warn(
                        "Failed to {} in Milvus metadata applier. Retry {}/{}.",
                        action,
                        attempt + 1,
                        config.getMaxRetries(),
                        e);
                sleepBeforeRetry(attempt);
            }
        }
        throw failure;
    }

    private void sleepBeforeRetry(int attempt) {
        long backoffMillis =
                MilvusRetryUtils.computeRetryBackoffMillis(
                        config.getRetryBackoff().toMillis(), attempt);
        if (backoffMillis <= 0) {
            return;
        }
        try {
            Thread.sleep(backoffMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while waiting to retry Milvus metadata operation.", e);
        }
    }

    private SchemaEvolveException unsupported(SchemaChangeEvent event) {
        TableId tableId = event.tableId();
        return new SchemaEvolveException(
                event,
                "Milvus sink does not support schema evolution type "
                        + event.getType()
                        + " for table "
                        + tableId
                        + ".",
                null);
    }

    @FunctionalInterface
    private interface RetryableOperation<T> {
        T run();
    }
}
