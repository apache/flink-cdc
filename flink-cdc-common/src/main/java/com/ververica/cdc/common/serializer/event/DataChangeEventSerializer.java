/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.common.serializer.event;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.event.DataChangeEvent;
import com.ververica.cdc.common.event.OperationType;
import com.ververica.cdc.common.event.TableId;
import com.ververica.cdc.common.serializer.EnumSerializer;
import com.ververica.cdc.common.serializer.MapSerializer;
import com.ververica.cdc.common.serializer.NullableSerializerWrapper;
import com.ververica.cdc.common.serializer.StringSerializer;
import com.ververica.cdc.common.serializer.TableIdSerializer;

import java.io.IOException;
import java.util.Map;

/** A {@link TypeSerializer} for {@link DataChangeEvent}. */
public class DataChangeEventSerializer extends TypeSerializer<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<RecordData> recordDataSerializer;
    private final TableIdSerializer tableIdSerializer = TableIdSerializer.INSTANCE;
    private final TypeSerializer<Map<String, String>> metaSerializer =
            new NullableSerializerWrapper<>(
                    new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE));
    private final EnumSerializer<OperationType> opSerializer =
            new EnumSerializer<>(OperationType.class);

    public DataChangeEventSerializer(TypeSerializer<RecordData> recordDataSerializer) {
        this.recordDataSerializer = recordDataSerializer;
    }

    @Override
    public TypeSerializer<DataChangeEvent> duplicate() {
        return new DataChangeEventSerializer(recordDataSerializer.duplicate());
    }

    @Override
    public DataChangeEvent createInstance() {
        // default use binary row to deserializer
        return DataChangeEvent.deleteEvent(TableId.tableId("unknown"), null);
    }

    @Override
    public void serialize(DataChangeEvent event, DataOutputView target) throws IOException {
        opSerializer.serialize(event.op(), target);
        tableIdSerializer.serialize(event.tableId(), target);
        if (event.before() != null) {
            recordDataSerializer.serialize(event.before(), target);
        }
        if (event.after() != null) {
            recordDataSerializer.serialize(event.after(), target);
        }
        metaSerializer.serialize(event.meta(), target);
    }

    @Override
    public DataChangeEvent deserialize(DataInputView source) throws IOException {
        OperationType op = opSerializer.deserialize(source);
        switch (op) {
            case DELETE:
                return DataChangeEvent.deleteEvent(
                        tableIdSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case INSERT:
                return DataChangeEvent.insertEvent(
                        tableIdSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case UPDATE:
                return DataChangeEvent.updateEvent(
                        tableIdSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            case REPLACE:
                return DataChangeEvent.replaceEvent(
                        tableIdSerializer.deserialize(source),
                        recordDataSerializer.deserialize(source),
                        metaSerializer.deserialize(source));
            default:
                throw new IllegalArgumentException("Unsupported data change event: " + op);
        }
    }

    @Override
    public DataChangeEvent deserialize(DataChangeEvent reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public DataChangeEvent copy(DataChangeEvent from) {
        OperationType op = from.op();
        switch (op) {
            case DELETE:
                return DataChangeEvent.deleteEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.before()),
                        metaSerializer.copy(from.meta()));
            case INSERT:
                return DataChangeEvent.insertEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            case UPDATE:
                return DataChangeEvent.updateEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.before()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            case REPLACE:
                return DataChangeEvent.replaceEvent(
                        tableIdSerializer.copy(from.tableId()),
                        recordDataSerializer.copy(from.after()),
                        metaSerializer.copy(from.meta()));
            default:
                throw new IllegalArgumentException("Unsupported data change event: " + op);
        }
    }

    @Override
    public DataChangeEvent copy(DataChangeEvent from, DataChangeEvent reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DataChangeEventSerializer) {
            DataChangeEventSerializer other = (DataChangeEventSerializer) obj;
            return recordDataSerializer.equals(other.recordDataSerializer);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return recordDataSerializer.hashCode();
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
    public TypeSerializerSnapshot<DataChangeEvent> snapshotConfiguration() {
        return new DataChangeEventSerializerSnapshot(recordDataSerializer);
    }

    /** {@link TypeSerializerSnapshot} for {@link DataChangeEventSerializer}. */
    public static final class DataChangeEventSerializerSnapshot
            implements TypeSerializerSnapshot<DataChangeEvent> {
        private static final int CURRENT_VERSION = 1;

        private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

        public DataChangeEventSerializerSnapshot() {
            // this constructor is used when restoring from a checkpoint/savepoint.
        }

        DataChangeEventSerializerSnapshot(TypeSerializer<RecordData> serializer) {
            this.nestedSerializersSnapshotDelegate =
                    new NestedSerializersSnapshotDelegate(serializer);
        }

        @Override
        public int getCurrentVersion() {
            return CURRENT_VERSION;
        }

        @Override
        public void writeSnapshot(DataOutputView dataOutputView) throws IOException {
            nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(dataOutputView);
        }

        @Override
        public void readSnapshot(
                int readVersion, DataInputView dataInputView, ClassLoader classLoader)
                throws IOException {
            this.nestedSerializersSnapshotDelegate =
                    NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
                            dataInputView, classLoader);
        }

        @Override
        public TypeSerializer<DataChangeEvent> restoreSerializer() {
            return new DataChangeEventSerializer(
                    (TypeSerializer<RecordData>)
                            nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()[0]);
        }

        @Override
        public TypeSerializerSchemaCompatibility<DataChangeEvent> resolveSchemaCompatibility(
                TypeSerializer<DataChangeEvent> newSerializer) {
            if (!(newSerializer instanceof DataChangeEventSerializer)) {
                return TypeSerializerSchemaCompatibility.incompatible();
            }

            CompositeTypeSerializerUtil.IntermediateCompatibilityResult<DataChangeEvent>
                    intermediateResult =
                            CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
                                    new TypeSerializer[] {
                                        ((DataChangeEventSerializer) newSerializer)
                                                .recordDataSerializer
                                    },
                                    nestedSerializersSnapshotDelegate
                                            .getNestedSerializerSnapshots());

            if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
                return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
                        restoreSerializer());
            }

            return intermediateResult.getFinalResult();
        }
    }
}
