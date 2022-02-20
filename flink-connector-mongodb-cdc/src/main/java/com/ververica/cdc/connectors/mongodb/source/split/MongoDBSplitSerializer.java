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

package com.ververica.cdc.connectors.mongodb.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mongodb.source.config.MongoDBChangeStreamConfig;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffsetSerializer;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import io.debezium.util.HexConverter;
import org.bson.BsonDocument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** A serializer for the {@link MongoDBSplit}. */
public final class MongoDBSplitSerializer implements SimpleVersionedSerializer<MongoDBSplit> {

    public static final MongoDBSplitSerializer INSTANCE = new MongoDBSplitSerializer();

    private static final int VERSION = 1;
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int STREAM_SPLIT_FLAG = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(MongoDBSplit split) throws IOException {
        if (split.isSnapshotSplit()) {
            final MongoDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
            // optimization: the splits lazily cache their own serialized form
            if (snapshotSplit.serializedFormCache != null) {
                return snapshotSplit.serializedFormCache;
            }

            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(SNAPSHOT_SPLIT_FLAG);
            out.writeUTF(snapshotSplit.splitId());
            out.writeUTF(snapshotSplit.getCollectionId().identifier());
            out.writeUTF(snapshotSplit.getMin().toJson());
            out.writeUTF(snapshotSplit.getMax().toJson());
            out.writeUTF(snapshotSplit.getHint().toJson());
            out.writeBoolean(snapshotSplit.isFinished());

            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            snapshotSplit.serializedFormCache = result;
            return result;
        } else {
            final MongoDBStreamSplit streamSplit = split.asStreamSplit();
            // optimization: the splits lazily cache their own serialized form
            if (streamSplit.serializedFormCache != null) {
                return streamSplit.serializedFormCache;
            }
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            out.writeInt(STREAM_SPLIT_FLAG);
            out.writeUTF(streamSplit.splitId());
            out.writeUTF(serializeChangeStreamConfig(streamSplit.getChangeStreamConfig()));
            out.writeUTF(serializeChangeStreamOffset(streamSplit.getChangeStreamOffset()));
            out.writeBoolean(streamSplit.isSuspended());
            final byte[] result = out.getCopyOfBuffer();
            out.clear();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            streamSplit.serializedFormCache = result;
            return result;
        }
    }

    @Override
    public MongoDBSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeSplit(version, serialized);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }

    public MongoDBSplit deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        int splitKind = in.readInt();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            String splitId = in.readUTF();
            CollectionId collectionId = CollectionId.parse(in.readUTF());
            BsonDocument min = BsonDocument.parse(in.readUTF());
            BsonDocument max = BsonDocument.parse(in.readUTF());
            BsonDocument hint = BsonDocument.parse(in.readUTF());
            boolean finished = in.readBoolean();

            return new MongoDBSnapshotSplit(splitId, collectionId, min, max, hint, finished);
        } else if (splitKind == STREAM_SPLIT_FLAG) {
            String splitId = in.readUTF();
            MongoDBChangeStreamConfig changeStreamConfig =
                    deserializeChangeStreamConfig(in.readUTF());
            MongoDBChangeStreamOffset changeStreamOffset =
                    deserializeChangeStreamOffset(in.readUTF());
            boolean suspended = in.readBoolean();
            return new MongoDBStreamSplit(
                    splitId, changeStreamConfig, changeStreamOffset, suspended);
        } else {
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }

    private String serializeChangeStreamConfig(MongoDBChangeStreamConfig changeStreamConfig)
            throws IOException {
        try (final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(changeStreamConfig);
            return HexConverter.convertToHexString(bos.toByteArray());
        }
    }

    private MongoDBChangeStreamConfig deserializeChangeStreamConfig(String serialized)
            throws IOException {
        try (final ByteArrayInputStream bis =
                        new ByteArrayInputStream(HexConverter.convertFromHex(serialized));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (MongoDBChangeStreamConfig) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "Deserialize MongoDBChangeStreamConfig failed, class not found ", e);
        }
    }

    private String serializeChangeStreamOffset(MongoDBChangeStreamOffset changeStreamOffset) {
        return MongoDBChangeStreamOffsetSerializer.serialize(changeStreamOffset);
    }

    private MongoDBChangeStreamOffset deserializeChangeStreamOffset(String serialized) {
        return MongoDBChangeStreamOffsetSerializer.deserialize(serialized);
    }
}
