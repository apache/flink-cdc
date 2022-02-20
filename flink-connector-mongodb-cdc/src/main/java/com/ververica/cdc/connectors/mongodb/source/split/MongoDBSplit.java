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

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

/** The split describes a snapshot chunk or a change stream of MongoDB. */
public abstract class MongoDBSplit implements SourceSplit {

    protected final String splitId;

    public MongoDBSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /** Checks whether this split is a snapshot split. */
    public final boolean isSnapshotSplit() {
        return getClass() == MongoDBSnapshotSplit.class;
    }

    /** Checks whether this split is a stream split. */
    public final boolean isStreamSplit() {
        return getClass() == MongoDBStreamSplit.class;
    }

    /** Casts this split into a {@link MongoDBSnapshotSplit}. */
    public final MongoDBSnapshotSplit asSnapshotSplit() {
        return (MongoDBSnapshotSplit) this;
    }

    /** Casts this split into a {@link MongoDBStreamSplit}. */
    public final MongoDBStreamSplit asStreamSplit() {
        return (MongoDBStreamSplit) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoDBSplit that = (MongoDBSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
