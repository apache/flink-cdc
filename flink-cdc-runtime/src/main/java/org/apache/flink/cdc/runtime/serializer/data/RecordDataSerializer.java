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

package org.apache.flink.cdc.runtime.serializer.data;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.data.GenericRecordData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.runtime.serializer.TypeSerializerSingleton;
import org.apache.flink.cdc.runtime.serializer.data.binary.BinaryRecordDataSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** Serializer for {@link RecordData}. */
public class RecordDataSerializer extends TypeSerializerSingleton<RecordData> {

    private static final long serialVersionUID = 1L;

    /** Type tag for BinaryRecordData serialization. */
    private static final byte BINARY_RECORD_TYPE = 0;

    /** Type tag for GenericRecordData serialization. */
    private static final byte GENERIC_RECORD_TYPE = 1;

    private final BinaryRecordDataSerializer binarySerializer = BinaryRecordDataSerializer.INSTANCE;

    public static final RecordDataSerializer INSTANCE = new RecordDataSerializer();

    @Override
    public RecordData createInstance() {
        return new BinaryRecordData(1);
    }

    @Override
    public void serialize(RecordData recordData, DataOutputView target) throws IOException {
        if (recordData instanceof BinaryRecordData) {
            target.writeByte(BINARY_RECORD_TYPE);
            binarySerializer.serialize((BinaryRecordData) recordData, target);
        } else if (recordData instanceof GenericRecordData) {
            target.writeByte(GENERIC_RECORD_TYPE);
            GenericRecordDataSerializer.serialize((GenericRecordData) recordData, target);
        } else {
            throw new IOException(
                    "Unsupported RecordData type: " + recordData.getClass().getName());
        }
    }

    @Override
    public RecordData deserialize(DataInputView source) throws IOException {
        byte type = source.readByte();
        if (type == BINARY_RECORD_TYPE) {
            return binarySerializer.deserialize(source);
        } else if (type == GENERIC_RECORD_TYPE) {
            return GenericRecordDataSerializer.deserialize(source);
        } else {
            throw new IOException("Unknown RecordData type tag: " + type);
        }
    }

    @Override
    public RecordData deserialize(RecordData reuse, DataInputView source) throws IOException {
        byte type = source.readByte();
        if (type == BINARY_RECORD_TYPE) {
            if (reuse instanceof BinaryRecordData) {
                return binarySerializer.deserialize((BinaryRecordData) reuse, source);
            }
            return binarySerializer.deserialize(source);
        } else if (type == GENERIC_RECORD_TYPE) {
            return GenericRecordDataSerializer.deserialize(source);
        } else {
            throw new IOException("Unknown RecordData type tag: " + type);
        }
    }

    @Override
    public RecordData copy(RecordData from) {
        if (from instanceof BinaryRecordData) {
            return ((BinaryRecordData) from).copy();
        } else if (from instanceof GenericRecordData) {
            return GenericRecordDataSerializer.copy((GenericRecordData) from);
        } else {
            throw new RuntimeException("Unsupported RecordData type: " + from.getClass().getName());
        }
    }

    @Override
    public RecordData copy(RecordData from, RecordData reuse) {
        if (from instanceof BinaryRecordData) {
            BinaryRecordData reuseRecord =
                    (reuse instanceof BinaryRecordData)
                            ? (BinaryRecordData) reuse
                            : new BinaryRecordData(from.getArity());
            return ((BinaryRecordData) from).copy(reuseRecord);
        } else if (from instanceof GenericRecordData) {
            GenericRecordData reuseRecord =
                    (reuse instanceof GenericRecordData)
                            ? (GenericRecordData) reuse
                            : new GenericRecordData(from.getArity());
            return GenericRecordDataSerializer.copy((GenericRecordData) from, reuseRecord);
        } else {
            throw new RuntimeException("Unsupported RecordData type: " + from.getClass().getName());
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<RecordData> snapshotConfiguration() {
        return new RecordDataSerializerSnapshot();
    }

    /** {@link TypeSerializerSnapshot} for {@link RecordDataSerializer}. */
    public static final class RecordDataSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<RecordData> {

        public RecordDataSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
