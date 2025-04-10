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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostTransformChangeInfo caches pre-transformed / pre-transformed schema, schema field getters,
 * and binary record data generator for post-transform schema.
 */
public class PostTransformChangeInfo {

    private final TableId tableId;

    private final Schema preTransformedSchema;
    private final Schema postTransformedSchema;
    private final Map<String, Integer> preTransformedSchemaFieldNameToIndexMap;

    private final RecordData.FieldGetter[] preTransformedFieldGetters;
    private final RecordData.FieldGetter[] postTransformedFieldGetters;
    private final BinaryRecordDataGenerator postTransformedRecordDataGenerator;
    private final Map<String, Integer> postTransformedSchemaFieldNameToIndexMap;

    public static PostTransformChangeInfo of(
            TableId tableId, Schema preTransformedSchema, Schema postTransformedSchema) {

        List<RecordData.FieldGetter> preTransformedFieldGetters =
                SchemaUtils.createFieldGetters(preTransformedSchema.getColumns());

        List<RecordData.FieldGetter> postTransformedFieldGetters =
                SchemaUtils.createFieldGetters(postTransformedSchema.getColumns());

        BinaryRecordDataGenerator postTransformedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(postTransformedSchema.getColumns()));

        return new PostTransformChangeInfo(
                tableId,
                preTransformedSchema,
                preTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                postTransformedSchema,
                postTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                postTransformedRecordDataGenerator);
    }

    private PostTransformChangeInfo(
            TableId tableId,
            Schema preTransformedSchema,
            RecordData.FieldGetter[] preTransformedFieldGetters,
            Schema postTransformedSchema,
            RecordData.FieldGetter[] postTransformedFieldGetters,
            BinaryRecordDataGenerator postTransformedRecordDataGenerator) {

        this.tableId = tableId;

        this.preTransformedSchema = preTransformedSchema;
        this.preTransformedFieldGetters = preTransformedFieldGetters;
        this.preTransformedSchemaFieldNameToIndexMap = new HashMap<>();
        for (int i = 0; i < preTransformedSchema.getColumns().size(); i++) {
            preTransformedSchemaFieldNameToIndexMap.put(
                    preTransformedSchema.getColumns().get(i).getName(), i);
        }

        this.postTransformedSchema = postTransformedSchema;
        this.postTransformedFieldGetters = postTransformedFieldGetters;
        this.postTransformedRecordDataGenerator = postTransformedRecordDataGenerator;
        this.postTransformedSchemaFieldNameToIndexMap = new HashMap<>();

        for (int i = 0; i < postTransformedSchema.getColumns().size(); i++) {
            postTransformedSchemaFieldNameToIndexMap.put(
                    postTransformedSchema.getColumns().get(i).getName(), i);
        }
    }

    public String getName() {
        return tableId.identifier();
    }

    public String getTableName() {
        return tableId.getTableName();
    }

    public String getSchemaName() {
        return tableId.getSchemaName();
    }

    public String getNamespace() {
        return tableId.getNamespace();
    }

    public TableId getTableId() {
        return tableId;
    }

    public Schema getPreTransformedSchema() {
        return preTransformedSchema;
    }

    public Schema getPostTransformedSchema() {
        return postTransformedSchema;
    }

    public @Nullable Integer getPreTransformedSchemaFieldIndex(String fieldName) {
        return preTransformedSchemaFieldNameToIndexMap.get(fieldName);
    }

    public @Nullable Integer getPostTransformedSchemaFieldIndex(String fieldName) {
        return postTransformedSchemaFieldNameToIndexMap.get(fieldName);
    }

    public RecordData.FieldGetter[] getPreTransformedFieldGetters() {
        return preTransformedFieldGetters;
    }

    public BinaryRecordDataGenerator getPostTransformedRecordDataGenerator() {
        return postTransformedRecordDataGenerator;
    }
}
