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

    private final BinaryRecordDataSerializer binarySerializer = BinaryRecordDataSerializer.INSTANCE;

    public static final RecordDataSerializer INSTANCE = new RecordDataSerializer();

    @Override
    public RecordData createInstance() {
        // BinaryRecordData is the only implementation of RecordData
        return new BinaryRecordData(1);
    }

    @Override
    public void serialize(RecordData recordData, DataOutputView target) throws IOException {
        // BinaryRecordData is the only implementation of RecordData
        binarySerializer.serialize((BinaryRecordData) recordData, target);
    }

    @Override
    public RecordData deserialize(DataInputView source) throws IOException {
        // BinaryRecordData is the only implementation of RecordData
        return binarySerializer.deserialize(source);
    }

    @Override
    public RecordData deserialize(RecordData reuse, DataInputView source) throws IOException {
        return binarySerializer.deserialize((BinaryRecordData) reuse, source);
    }

    @Override
    public RecordData copy(RecordData from) {
        // BinaryRecordData is the only implementation of RecordData
        return ((BinaryRecordData) from).copy();
    }

    @Override
    public RecordData copy(RecordData from, RecordData reuse) {
        // BinaryRecordData is the only implementation of RecordData
        return ((BinaryRecordData) from).copy((BinaryRecordData) reuse);
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
