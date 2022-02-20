/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.assigners.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mongodb.source.assigners.AssignerStatus;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSnapshotSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBSplit;
import com.ververica.cdc.connectors.mongodb.source.split.MongoDBStreamSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState} of MongoDB
 * CDC source.
 */
public class PendingSplitsStateSerializer implements SimpleVersionedSerializer<PendingSplitsState> {

    private static final int VERSION = 1;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final SimpleVersionedSerializer<MongoDBSplit> splitSerializer;

    public PendingSplitsStateSerializer(SimpleVersionedSerializer<MongoDBSplit> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(PendingSplitsState state) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeInt(splitSerializer.getVersion());
        serializePendingSplitsState(state, out);

        final byte[] result = out.getCopyOfBuffer();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        state.serializedFormCache = result;
        out.clear();
        return result;
    }

    @Override
    public PendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializePendingSplitsState(serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Serialize
    // ------------------------------------------------------------------------------------------

    private void serializePendingSplitsState(PendingSplitsState state, DataOutputSerializer out)
            throws IOException {
        out.writeBoolean(state.isStreamSplitAssigned());
        writeCollectionIds(state.getAlreadyProcessedCollections(), out);
        writeCollectionIds(state.getRemainingCollections(), out);
        writeMongoDBSplits(state.getRemainingSnapshotSplits(), out);
        writeAssignedSnapshotSplits(state.getAssignedSnapshotSplits(), out);
        writeFinishedSnapshotSplits(state.getFinishedSnapshotSplits(), out);
        writeMongoDBSplit(state.getRemainingStreamSplit(), out);
        out.writeInt(state.getAssignerStatus().getStatusCode());
    }

    // ------------------------------------------------------------------------------------------
    // Deserialize
    // ------------------------------------------------------------------------------------------

    public PendingSplitsState deserializePendingSplitsState(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        return deserializePendingSplitsState(splitVersion, in);
    }

    private PendingSplitsState deserializePendingSplitsState(
            int splitVersion, DataInputDeserializer in) throws IOException {

        boolean isStreamSplitAssigned = in.readBoolean();
        List<CollectionId> alreadyProcessedCollections = readCollectionIds(in);
        List<CollectionId> remainingCollections = readCollectionIds(in);

        List<MongoDBSnapshotSplit> remainingSnapshotSplits =
                readMongoDBSnapshotSplits(splitVersion, in);
        Map<String, MongoDBSnapshotSplit> assignedSnapshotSplits =
                readAssignedSnapshotSplits(splitVersion, in);
        List<String> finishedSnapshotSplits = readFinishedSnapshotSplits(in);

        MongoDBStreamSplit remainingStreamSplit =
                Optional.ofNullable(readMongoDBSplit(splitVersion, in))
                        .map(MongoDBSplit::asStreamSplit)
                        .orElse(null);
        AssignerStatus assignerStatus = AssignerStatus.fromStatusCode(in.readInt());

        return new PendingSplitsState(
                isStreamSplitAssigned,
                remainingCollections,
                alreadyProcessedCollections,
                remainingSnapshotSplits,
                assignedSnapshotSplits,
                finishedSnapshotSplits,
                remainingStreamSplit,
                assignerStatus);
    }

    // ------------------------------------------------------------------------------------------
    // Utilities
    // ------------------------------------------------------------------------------------------

    private void writeFinishedSnapshotSplits(
            List<String> finishedSnapshotSplits, DataOutputSerializer out) throws IOException {
        final int size = finishedSnapshotSplits.size();
        out.writeInt(size);
        for (String splitId : finishedSnapshotSplits) {
            out.writeUTF(splitId);
        }
    }

    private List<String> readFinishedSnapshotSplits(DataInputDeserializer in) throws IOException {
        List<String> finishedSnapshotSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            finishedSnapshotSplits.add(in.readUTF());
        }
        return finishedSnapshotSplits;
    }

    private void writeAssignedSnapshotSplits(
            Map<String, MongoDBSnapshotSplit> assignedSplits, DataOutputSerializer out)
            throws IOException {
        final int size = assignedSplits.size();
        out.writeInt(size);
        for (Map.Entry<String, MongoDBSnapshotSplit> entry : assignedSplits.entrySet()) {
            out.writeUTF(entry.getKey());
            byte[] splitBytes = splitSerializer.serialize(entry.getValue());
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private Map<String, MongoDBSnapshotSplit> readAssignedSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        Map<String, MongoDBSnapshotSplit> assignedSplits = new HashMap<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String splitId = in.readUTF();
            MongoDBSnapshotSplit snapshotSplit =
                    readMongoDBSplit(splitVersion, in).asSnapshotSplit();
            assignedSplits.put(splitId, snapshotSplit);
        }
        return assignedSplits;
    }

    private <T extends MongoDBSplit> void writeMongoDBSplits(
            Collection<T> mongoDBSplits, DataOutputSerializer out) throws IOException {
        final int size = mongoDBSplits.size();
        out.writeInt(size);
        for (MongoDBSplit split : mongoDBSplits) {
            writeMongoDBSplit(split, out);
        }
    }

    private <T extends MongoDBSplit> void writeMongoDBSplit(
            T mongoDBSplit, DataOutputSerializer out) throws IOException {
        if (mongoDBSplit == null) {
            out.writeInt(0);
        } else {
            byte[] splitBytes = splitSerializer.serialize(mongoDBSplit);
            out.writeInt(splitBytes.length);
            out.write(splitBytes);
        }
    }

    private List<MongoDBSnapshotSplit> readMongoDBSnapshotSplits(
            int splitVersion, DataInputDeserializer in) throws IOException {
        List<MongoDBSnapshotSplit> mongoDBSplits = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            MongoDBSnapshotSplit mongoDBSplit =
                    Optional.ofNullable(readMongoDBSplit(splitVersion, in))
                            .map(MongoDBSplit::asSnapshotSplit)
                            .orElse(null);
            mongoDBSplits.add(mongoDBSplit);
        }
        return mongoDBSplits;
    }

    private MongoDBSplit readMongoDBSplit(int splitVersion, DataInputDeserializer in)
            throws IOException {
        int splitBytesLen = in.readInt();
        if (splitBytesLen != 0) {
            byte[] splitBytes = new byte[splitBytesLen];
            in.read(splitBytes);
            return splitSerializer.deserialize(splitVersion, splitBytes);
        } else {
            return null;
        }
    }

    private void writeCollectionIds(
            Collection<CollectionId> collectionIds, DataOutputSerializer out) throws IOException {
        final int size = collectionIds.size();
        out.writeInt(size);
        for (CollectionId collectionId : collectionIds) {
            out.writeUTF(collectionId.identifier());
        }
    }

    private List<CollectionId> readCollectionIds(DataInputDeserializer in) throws IOException {
        List<CollectionId> collectionIds = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            collectionIds.add(CollectionId.parse(in.readUTF()));
        }
        return collectionIds;
    }
}
