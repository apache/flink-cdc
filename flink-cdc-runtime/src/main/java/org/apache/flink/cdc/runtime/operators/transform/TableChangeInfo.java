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
import org.apache.flink.cdc.runtime.serializer.TableIdSerializer;
import org.apache.flink.cdc.runtime.serializer.schema.SchemaSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/** The TableInfo applies to cache schema change and fieldGetters. */
public class TableChangeInfo {
    private TableId tableId;
    private Schema originalSchema;
    private Schema transformedSchema;
    private RecordData.FieldGetter[] fieldGetters;
    private BinaryRecordDataGenerator recordDataGenerator;

    public static final TableChangeInfo.Serializer SERIALIZER = new TableChangeInfo.Serializer();

    public TableChangeInfo(
            TableId tableId,
            Schema originalSchema,
            Schema transformedSchema,
            RecordData.FieldGetter[] fieldGetters,
            BinaryRecordDataGenerator recordDataGenerator) {
        this.tableId = tableId;
        this.originalSchema = originalSchema;
        this.transformedSchema = transformedSchema;
        this.fieldGetters = fieldGetters;
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

    public TableId getTableId() {
        return tableId;
    }

    public Schema getOriginalSchema() {
        return originalSchema;
    }

    public Schema getTransformedSchema() {
        return transformedSchema;
    }

    public RecordData.FieldGetter[] getFieldGetters() {
        return fieldGetters;
    }

    public BinaryRecordDataGenerator getRecordDataGenerator() {
        return recordDataGenerator;
    }

    public static TableChangeInfo of(
            TableId tableId, Schema originalSchema, Schema transformedSchema) {
        List<RecordData.FieldGetter> fieldGetters =
                SchemaUtils.createFieldGetters(originalSchema.getColumns());
        BinaryRecordDataGenerator recordDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(transformedSchema.getColumns()));
        return new TableChangeInfo(
                tableId,
                originalSchema,
                transformedSchema,
                fieldGetters.toArray(new RecordData.FieldGetter[0]),
                recordDataGenerator);
    }

    /** Serializer for {@link TableChangeInfo}. */
    public static class Serializer implements SimpleVersionedSerializer<TableChangeInfo> {

        public static final int CURRENT_VERSION = 1;

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(TableChangeInfo tableChangeInfo) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                tableIdSerializer.serialize(
                        tableChangeInfo.getTableId(), new DataOutputViewStreamWrapper(out));
                schemaSerializer.serialize(
                        tableChangeInfo.originalSchema, new DataOutputViewStreamWrapper(out));
                schemaSerializer.serialize(
                        tableChangeInfo.transformedSchema, new DataOutputViewStreamWrapper(out));
                return baos.toByteArray();
            }
        }

        @Override
        public TableChangeInfo deserialize(int version, byte[] serialized) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                Schema originalSchema =
                        schemaSerializer.deserialize(version, new DataInputViewStreamWrapper(in));
                Schema transformedSchema =
                        schemaSerializer.deserialize(version, new DataInputViewStreamWrapper(in));
                return TableChangeInfo.of(tableId, originalSchema, transformedSchema);
            }
        }
    }
}
