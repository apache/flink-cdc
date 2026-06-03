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

package org.apache.flink.cdc.connectors.milvus.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.milvus.sink.MilvusDataSinkConfig;

import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Utilities for Milvus collection schema construction and validation. */
public class MilvusCollectionUtils {

    private MilvusCollectionUtils() {}

    public static CreateCollectionReq buildCreateCollectionReq(
            TableId tableId, Schema schema, MilvusDataSinkConfig config) {
        String collectionName = resolveCollectionName(tableId, config);
        String primaryKey = resolvePrimaryKey(schema, config);
        List<MilvusVectorFieldSpec> vectorFields = config.getVectorFields(tableId);
        validateCreateCollectionSchema(tableId, schema, primaryKey, vectorFields);
        validatePartitionField(tableId, schema, config);

        CreateCollectionReq.CollectionSchema collectionSchema =
                CreateCollectionReq.CollectionSchema.builder()
                        .enableDynamicField(config.isEnableDynamicField())
                        .build();

        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                continue;
            }
            Optional<MilvusVectorFieldSpec> vectorField =
                    MilvusTypeUtils.findVectorFieldSpec(column.getName(), vectorFields);
            collectionSchema.addField(
                    MilvusTypeUtils.toAddFieldReq(
                            column,
                            column.getName().equals(primaryKey),
                            vectorField,
                            config.getVarcharMaxLengthDefault()));
        }

        CreateCollectionReq.CreateCollectionReqBuilder builder =
                CreateCollectionReq.builder()
                        .databaseName(config.getDatabaseName())
                        .collectionName(collectionName)
                        .collectionSchema(collectionSchema)
                        .enableDynamicField(config.isEnableDynamicField());

        Optional<ConsistencyLevel> consistencyLevel = parseConsistencyLevel(config);
        consistencyLevel.ifPresent(builder::consistencyLevel);

        if (config.isCreateIndexEnabled()) {
            builder.indexParams(buildIndexParams(vectorFields, config));
        }
        return builder.build();
    }

    public static List<IndexParam> buildIndexParams(
            List<MilvusVectorFieldSpec> vectorFields, MilvusDataSinkConfig config) {
        List<IndexParam> indexParams = new ArrayList<>();
        IndexParam.IndexType indexType =
                IndexParam.IndexType.valueOf(config.getIndexType().toUpperCase(Locale.ROOT));
        IndexParam.MetricType metricType =
                IndexParam.MetricType.valueOf(config.getIndexMetricType().toUpperCase(Locale.ROOT));
        for (MilvusVectorFieldSpec vectorField : vectorFields) {
            indexParams.add(
                    IndexParam.builder()
                            .fieldName(vectorField.getFieldName())
                            .indexName(MilvusNameUtils.indexName(vectorField.getFieldName()))
                            .indexType(indexType)
                            .metricType(metricType)
                            .extraParams(config.getIndexParams())
                            .build());
        }
        return indexParams;
    }

    public static void validateExistingCollection(
            TableId tableId,
            Schema schema,
            DescribeCollectionResp existing,
            MilvusDataSinkConfig config) {
        String primaryKey = resolvePrimaryKey(schema, config);
        validatePartitionField(tableId, schema, config);
        if (!primaryKey.equals(existing.getPrimaryFieldName())) {
            throw new IllegalStateException(
                    "Existing Milvus collection "
                            + existing.getCollectionName()
                            + " primary key mismatch. Expected "
                            + primaryKey
                            + " but was "
                            + existing.getPrimaryFieldName()
                            + ".");
        }
        CreateCollectionReq.CollectionSchema collectionSchema = existing.getCollectionSchema();
        if (collectionSchema == null) {
            return;
        }
        CreateCollectionReq.FieldSchema primaryFieldSchema = collectionSchema.getField(primaryKey);
        if (primaryFieldSchema == null) {
            throw new IllegalStateException(
                    "Existing Milvus collection "
                            + existing.getCollectionName()
                            + " misses primary key field "
                            + primaryKey
                            + ".");
        }
        DataType expectedPrimaryKeyType =
                MilvusTypeUtils.toMilvusType(
                        schema.getColumn(primaryKey)
                                .orElseThrow(
                                        () ->
                                                new IllegalStateException(
                                                        "Primary key column "
                                                                + primaryKey
                                                                + " is absent from schema."))
                                .getType(),
                        true);
        if (primaryFieldSchema.getDataType() != expectedPrimaryKeyType) {
            throw new IllegalStateException(
                    "Existing Milvus primary key field "
                            + primaryKey
                            + " type mismatch. Expected "
                            + expectedPrimaryKeyType
                            + " but was "
                            + primaryFieldSchema.getDataType()
                            + ".");
        }
        if (Boolean.TRUE.equals(primaryFieldSchema.getAutoID())) {
            throw new IllegalStateException(
                    "Existing Milvus primary key field "
                            + primaryKey
                            + " uses autoID, but CDC upsert mode requires autoID=false.");
        }
        for (MilvusVectorFieldSpec vectorField : config.getVectorFields(tableId)) {
            CreateCollectionReq.FieldSchema fieldSchema =
                    collectionSchema.getField(vectorField.getFieldName());
            if (fieldSchema == null) {
                throw new IllegalStateException(
                        "Existing Milvus collection "
                                + existing.getCollectionName()
                                + " misses vector field "
                                + vectorField.getFieldName()
                                + ".");
            }
            if (fieldSchema.getDataType() != vectorField.getDataType()) {
                throw new IllegalStateException(
                        "Existing Milvus vector field "
                                + vectorField.getFieldName()
                                + " type mismatch. Expected "
                                + vectorField.getDataType()
                                + " but was "
                                + fieldSchema.getDataType()
                                + ".");
            }
            if (!Integer.valueOf(vectorField.getDimension()).equals(fieldSchema.getDimension())) {
                throw new IllegalStateException(
                        "Existing Milvus vector field "
                                + vectorField.getFieldName()
                                + " dimension mismatch. Expected "
                                + vectorField.getDimension()
                                + " but was "
                                + fieldSchema.getDimension()
                                + ".");
            }
        }
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()
                    || column.getName().equals(primaryKey)
                    || MilvusTypeUtils.findVectorFieldSpec(
                                    column.getName(), config.getVectorFields(tableId))
                            .isPresent()) {
                continue;
            }
            CreateCollectionReq.FieldSchema fieldSchema =
                    collectionSchema.getField(column.getName());
            if (fieldSchema == null) {
                throw new IllegalStateException(
                        "Existing Milvus collection "
                                + existing.getCollectionName()
                                + " misses scalar field "
                                + column.getName()
                                + ".");
            }
            DataType expectedType = MilvusTypeUtils.toMilvusType(column.getType(), false);
            if (fieldSchema.getDataType() != expectedType) {
                throw new IllegalStateException(
                        "Existing Milvus scalar field "
                                + column.getName()
                                + " type mismatch. Expected "
                                + expectedType
                                + " but was "
                                + fieldSchema.getDataType()
                                + ".");
            }
            if (expectedType == DataType.VarChar
                    && fieldSchema.getMaxLength() != null
                    && fieldSchema.getMaxLength()
                            < MilvusTypeUtils.resolveVarcharMaxLength(
                                    column.getType(), config.getVarcharMaxLengthDefault())) {
                throw new IllegalStateException(
                        "Existing Milvus scalar field "
                                + column.getName()
                                + " max length is too small. Expected at least "
                                + MilvusTypeUtils.resolveVarcharMaxLength(
                                        column.getType(), config.getVarcharMaxLengthDefault())
                                + " but was "
                                + fieldSchema.getMaxLength()
                                + ".");
            }
        }
    }

    public static AddFieldReq buildAddScalarFieldReq(Column column, MilvusDataSinkConfig config) {
        MilvusNameUtils.validateIdentifier(column.getName(), "field");
        if (!column.getType().isNullable()) {
            throw new IllegalStateException(
                    "Adding NOT NULL Milvus scalar field "
                            + column.getName()
                            + " is not supported because existing rows cannot be backfilled by the connector.");
        }
        Optional<MilvusVectorFieldSpec> vectorField = Optional.empty();
        AddFieldReq req =
                MilvusTypeUtils.toAddFieldReq(
                        column, false, vectorField, config.getVarcharMaxLengthDefault());
        if (req.getDataType() == DataType.FloatVector) {
            throw new IllegalStateException("Adding Milvus vector fields is not supported.");
        }
        return req;
    }

    private static void validateCreateCollectionSchema(
            TableId tableId,
            Schema schema,
            String primaryKey,
            List<MilvusVectorFieldSpec> vectorFields) {
        Set<String> physicalColumns = new HashSet<>();
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                continue;
            }
            MilvusNameUtils.validateIdentifier(column.getName(), "field");
            physicalColumns.add(column.getName());
            Optional<MilvusVectorFieldSpec> vectorField =
                    MilvusTypeUtils.findVectorFieldSpec(column.getName(), vectorFields);
            if (vectorField.isPresent()) {
                validateVectorSourceColumn(tableId, column, primaryKey, vectorField.get());
            }
        }
        for (MilvusVectorFieldSpec vectorField : vectorFields) {
            if (!physicalColumns.contains(vectorField.getFieldName())) {
                throw new IllegalStateException(
                        "Configured Milvus vector field "
                                + vectorField.getFieldName()
                                + " is absent from table "
                                + tableId
                                + " schema.");
            }
        }
    }

    private static void validatePartitionField(
            TableId tableId, Schema schema, MilvusDataSinkConfig config) {
        String partitionField = config.getPartitionField();
        if (partitionField == null || partitionField.trim().isEmpty()) {
            return;
        }
        String field = partitionField.trim();
        Column column =
                schema.getColumn(field)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Configured Milvus partition field "
                                                        + field
                                                        + " is absent from table "
                                                        + tableId
                                                        + " schema."));
        if (MilvusTypeUtils.findVectorFieldSpec(field, config.getVectorFields(tableId))
                .isPresent()) {
            throw new IllegalStateException(
                    "Configured Milvus partition field "
                            + field
                            + " cannot also be a vector field.");
        }
        MilvusTypeUtils.toMilvusType(column.getType(), false);
    }

    private static void validateVectorSourceColumn(
            TableId tableId, Column column, String primaryKey, MilvusVectorFieldSpec vectorField) {
        if (column.getName().equals(primaryKey)) {
            throw new IllegalStateException(
                    "Milvus vector field "
                            + vectorField.getFieldName()
                            + " cannot also be the primary key of table "
                            + tableId
                            + ".");
        }
        if (column.getType().isNullable()) {
            throw new IllegalStateException(
                    "Milvus vector field "
                            + vectorField.getFieldName()
                            + " must be NOT NULL because Milvus vector fields cannot be null.");
        }
        if (!MilvusTypeUtils.isSupportedVectorSourceType(column.getType())) {
            throw new IllegalStateException(
                    "Milvus vector field "
                            + vectorField.getFieldName()
                            + " must be backed by ARRAY<FLOAT>, ARRAY<DOUBLE>, or a JSON array string column, but table "
                            + tableId
                            + " column type is "
                            + column.getType().asSummaryString()
                            + ".");
        }
    }

    public static String resolveCollectionName(TableId tableId, MilvusDataSinkConfig config) {
        return MilvusNameUtils.resolveCollectionName(
                tableId, config.getCollectionMappings(), config.isCollectionNameNormalizeEnabled());
    }

    public static void validateNormalizedCollectionCollisions(
            Collection<TableId> tableIds, MilvusDataSinkConfig config) {
        Map<String, TableId> seen = new HashMap<>();
        for (TableId tableId : tableIds) {
            registerCollectionName(tableId, config, seen);
        }
    }

    public static void registerCollectionName(
            TableId tableId, MilvusDataSinkConfig config, Map<String, TableId> seen) {
        String collectionName = resolveCollectionName(tableId, config);
        TableId previous = seen.put(collectionName, tableId);
        if (previous != null && !previous.equals(tableId)) {
            throw new IllegalStateException(
                    "Milvus collection name collision: "
                            + previous
                            + " and "
                            + tableId
                            + " both map to collection "
                            + collectionName
                            + ". Configure collection.mapping explicitly.");
        }
    }

    public static String resolvePrimaryKey(Schema schema, MilvusDataSinkConfig config) {
        String configuredPrimaryKey = config.getPrimaryKeyField();
        if (configuredPrimaryKey != null && !configuredPrimaryKey.trim().isEmpty()) {
            String primaryKey = configuredPrimaryKey.trim();
            Column column =
                    schema.getColumn(primaryKey)
                            .orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    "Configured primary key field "
                                                            + primaryKey
                                                            + " is absent from schema."));
            validatePrimaryKeyColumn(column);
            return primaryKey;
        }
        if (schema.primaryKeys().size() != 1) {
            if (schema.primaryKeys().isEmpty() && config.isAllowNoPrimaryKey()) {
                throw new IllegalStateException(
                        "Milvus auto generated IDs are not supported in CDC upsert mode. Please configure a primary key.");
            }
            throw new IllegalStateException(
                    "Milvus sink requires exactly one primary key, but table has "
                            + schema.primaryKeys().size()
                            + ".");
        }
        String primaryKey = schema.primaryKeys().get(0);
        Column column =
                schema.getColumn(primaryKey)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Primary key column "
                                                        + primaryKey
                                                        + " is absent from schema."));
        validatePrimaryKeyColumn(column);
        return primaryKey;
    }

    private static void validatePrimaryKeyColumn(Column column) {
        if (!MilvusTypeUtils.isSupportedPrimaryKeyType(column.getType())) {
            throw new IllegalStateException(
                    "Milvus sink only supports BIGINT, CHAR, and VARCHAR primary keys. Column "
                            + column.getName()
                            + " is "
                            + column.getType().asSummaryString()
                            + ".");
        }
        if (column.getType().isNullable()) {
            throw new IllegalStateException(
                    "Milvus primary key column " + column.getName() + " must be NOT NULL.");
        }
    }

    private static Optional<ConsistencyLevel> parseConsistencyLevel(MilvusDataSinkConfig config) {
        String consistencyLevel = config.getConsistencyLevel();
        if (consistencyLevel == null || consistencyLevel.trim().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                ConsistencyLevel.valueOf(consistencyLevel.trim().toUpperCase(Locale.ROOT)));
    }
}
