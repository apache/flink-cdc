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

import java.util.List;

/**
 * PostTransformChangeInfo caches pre-transformed / pre-transformed schema, schema field getters,
 * and binary record data generator for post-transform schema.
 */
public class PostTransformChangeInfo {
    private TableId tableId;
    private Schema preTransformedSchema;
    private Schema postTransformedSchema;
    private RecordData.FieldGetter[] preTransformedFieldGetters;
    private RecordData.FieldGetter[] postTransformedFieldGetters;
    private BinaryRecordDataGenerator recordDataGenerator;

    public PostTransformChangeInfo(
            TableId tableId,
            Schema postTransformedSchema,
            RecordData.FieldGetter[] postTransformedFieldGetters,
            Schema preTransformedSchema,
            RecordData.FieldGetter[] preTransformedFieldGetters,
            BinaryRecordDataGenerator recordDataGenerator) {
        this.tableId = tableId;
        this.postTransformedSchema = postTransformedSchema;
        this.postTransformedFieldGetters = postTransformedFieldGetters;
        this.preTransformedSchema = preTransformedSchema;
        this.preTransformedFieldGetters = preTransformedFieldGetters;
        this.recordDataGenerator = recordDataGenerator;
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

    public Schema getPostTransformedSchema() {
        return postTransformedSchema;
    }

    public Schema getPreTransformedSchema() {
        return preTransformedSchema;
    }

    public RecordData.FieldGetter[] getPostTransformedFieldGetters() {
        return postTransformedFieldGetters;
    }

    public RecordData.FieldGetter[] getPreTransformedFieldGetters() {
        return preTransformedFieldGetters;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    public static PostTransformChangeInfo of(
            TableId tableId, Schema postTransformedSchema, Schema preTransformedSchema) {

        List<RecordData.FieldGetter> postTransformedFieldGetters =
                SchemaUtils.createFieldGetters(postTransformedSchema.getColumns());

        List<RecordData.FieldGetter> preTransformedFieldGetters =
                SchemaUtils.createFieldGetters(preTransformedSchema.getColumns());

        BinaryRecordDataGenerator postTransformedRecordDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(postTransformedSchema.getColumns()));

        return new PostTransformChangeInfo(
                tableId,
                postTransformedSchema,
                postTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                preTransformedSchema,
                preTransformedFieldGetters.toArray(new RecordData.FieldGetter[0]),
                postTransformedRecordDataGenerator);
    }
}
