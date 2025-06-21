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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PreTransformChangeInfo caches source / pre-transformed schema, source schema field getters, and
 * binary record data generator for pre-transform schema.
 */
public class PreTransformChangeInfo {
    private final TableId tableId;
    private final Schema sourceSchema;
    private final Schema preTransformedSchema;
    private final Map<String, RecordData.FieldGetter> sourceFieldGettersMap;
    private final BinaryRecordDataGenerator preTransformedRecordDataGenerator;

    public static final PreTransformChangeInfo.Serializer SERIALIZER =
            new PreTransformChangeInfo.Serializer();

    public PreTransformChangeInfo(
            TableId tableId,
            Schema sourceSchema,
            Schema preTransformedSchema,
            RecordData.FieldGetter[] sourceFieldGettersMap,
            BinaryRecordDataGenerator preTransformedRecordDataGenerator) {
        this.tableId = tableId;
        this.sourceSchema = sourceSchema;
        this.preTransformedSchema = preTransformedSchema;
        this.sourceFieldGettersMap = new HashMap<>(sourceSchema.getColumnCount());
        for (int i = 0; i < sourceSchema.getColumns().size(); i++) {
            this.sourceFieldGettersMap.put(
                    sourceSchema.getColumns().get(i).getName(), sourceFieldGettersMap[i]);
        }
        this.preTransformedRecordDataGenerator = preTransformedRecordDataGenerator;
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

    public Schema getSourceSchema() {
        return sourceSchema;
    }

    public Schema getPreTransformedSchema() {
        return preTransformedSchema;
    }

    public Map<String, RecordData.FieldGetter> getSourceFieldGettersMap() {
        return sourceFieldGettersMap;
    }

    public BinaryRecordDataGenerator getPreTransformedRecordDataGenerator() {
        return preTransformedRecordDataGenerator;
    }

    public static PreTransformChangeInfo of(
            TableId tableId, Schema sourceSchema, Schema preTransformedSchema) {
        List<RecordData.FieldGetter> sourceFieldGetters =
                SchemaUtils.createFieldGetters(sourceSchema.getColumns());
        BinaryRecordDataGenerator preTransformedDataGenerator =
                new BinaryRecordDataGenerator(
                        DataTypeConverter.toRowType(preTransformedSchema.getColumns()));
        return new PreTransformChangeInfo(
                tableId,
                sourceSchema,
                preTransformedSchema,
                sourceFieldGetters.toArray(new RecordData.FieldGetter[0]),
                preTransformedDataGenerator);
    }

    /** Serializer for {@link PreTransformChangeInfo}. */
    public static class Serializer implements SimpleVersionedSerializer<PreTransformChangeInfo> {

        /** The latest version before change of state compatibility. */
        public static final int VERSION_BEFORE_STATE_COMPATIBILITY = 1;

        public static final int CURRENT_VERSION = 2;

        /** Used to distinguish with the state which CURRENT_VERSION was not written. */
        public static final TableId MAGIC_TABLE_ID = TableId.tableId("__magic_table__");

        @Override
        public int getVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public byte[] serialize(PreTransformChangeInfo tableChangeInfo) throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    DataOutputStream out = new DataOutputStream(baos)) {
                tableIdSerializer.serialize(MAGIC_TABLE_ID, new DataOutputViewStreamWrapper(out));
                out.writeInt(CURRENT_VERSION);
                tableIdSerializer.serialize(
                        tableChangeInfo.getTableId(), new DataOutputViewStreamWrapper(out));
                schemaSerializer.serialize(
                        tableChangeInfo.sourceSchema, new DataOutputViewStreamWrapper(out));
                schemaSerializer.serialize(
                        tableChangeInfo.preTransformedSchema, new DataOutputViewStreamWrapper(out));
                return baos.toByteArray();
            }
        }

        @Override
        public PreTransformChangeInfo deserialize(int version, byte[] serialized)
                throws IOException {
            TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
            SchemaSerializer schemaSerializer = SchemaSerializer.INSTANCE;
            try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                    DataInputStream in = new DataInputStream(bais)) {
                TableId tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                if (tableId.equals(MAGIC_TABLE_ID)) {
                    version = in.readInt();
                    tableId = tableIdSerializer.deserialize(new DataInputViewStreamWrapper(in));
                } else {
                    version = VERSION_BEFORE_STATE_COMPATIBILITY;
                }
                Schema originalSchema =
                        schemaSerializer.deserialize(version, new DataInputViewStreamWrapper(in));
                Schema transformedSchema =
                        schemaSerializer.deserialize(version, new DataInputViewStreamWrapper(in));
                return PreTransformChangeInfo.of(tableId, originalSchema, transformedSchema);
            }
        }
    }
}
