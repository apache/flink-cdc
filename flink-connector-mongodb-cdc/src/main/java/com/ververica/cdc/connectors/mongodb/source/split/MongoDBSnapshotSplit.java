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

import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import org.bson.BsonDocument;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_HINT;

/** The split to describe a split of a MongoDB collection snapshot. */
public class MongoDBSnapshotSplit extends MongoDBSplit {

    private final CollectionId collectionId;

    private final BsonDocument min;

    private final BsonDocument max;

    private final BsonDocument hint;

    private final boolean finished;

    @Nullable transient byte[] serializedFormCache;

    public MongoDBSnapshotSplit(
            String splitId,
            CollectionId collectionId,
            BsonDocument min,
            BsonDocument max,
            BsonDocument hint) {
        this(splitId, collectionId, min, max, hint, false);
    }

    public MongoDBSnapshotSplit(
            String splitId,
            CollectionId collectionId,
            BsonDocument min,
            BsonDocument max,
            BsonDocument hint,
            boolean finished) {
        super(splitId);
        this.collectionId = collectionId;
        this.min = min;
        this.max = max;
        this.hint = hint;
        this.finished = finished;
    }

    public CollectionId getCollectionId() {
        return collectionId;
    }

    public BsonDocument getMin() {
        return min;
    }

    public BsonDocument getMax() {
        return max;
    }

    public BsonDocument getHint() {
        return hint;
    }

    public boolean isFinished() {
        return finished;
    }

    public static MongoDBSnapshotSplit of(
            CollectionId collectionId, int chunkId, BsonDocument min, BsonDocument max) {
        return of(collectionId, chunkId, min, max, ID_HINT);
    }

    public static MongoDBSnapshotSplit of(
            CollectionId collectionId,
            int chunkId,
            BsonDocument min,
            BsonDocument max,
            BsonDocument hint) {
        String splitId = collectionId.identifier() + ":" + chunkId;
        return new MongoDBSnapshotSplit(splitId, collectionId, min, max, hint);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoDBSnapshotSplit that = (MongoDBSnapshotSplit) o;
        return Objects.equals(collectionId, that.collectionId)
                && Objects.equals(min, that.min)
                && Objects.equals(max, that.max)
                && Objects.equals(hint, that.hint)
                && Objects.equals(finished, that.finished);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), collectionId, min, max, hint, finished);
        result = 31 * result + Arrays.hashCode(serializedFormCache);
        return result;
    }

    @Override
    public String toString() {
        return "MongoDBSnapshotSplit{"
                + "collectionId="
                + collectionId
                + ", splitId='"
                + splitId
                + ", isFinished='"
                + finished
                + '\''
                + ", min="
                + min.toJson()
                + ", max="
                + max.toJson()
                + ", hint="
                + hint.toJson()
                + '}';
    }
}
