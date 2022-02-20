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

import com.ververica.cdc.connectors.mongodb.source.config.MongoDBChangeStreamConfig;
import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;

import javax.annotation.Nullable;

import java.util.Objects;

/** The split to describe the change stream of MongoDB. */
public class MongoDBStreamSplit extends MongoDBSplit {

    private final MongoDBChangeStreamConfig config;

    private final MongoDBChangeStreamOffset offset;

    private final boolean suspended;

    @Nullable transient byte[] serializedFormCache;

    public MongoDBStreamSplit(
            String splitId, MongoDBChangeStreamConfig config, MongoDBChangeStreamOffset offset) {
        this(splitId, config, offset, false);
    }

    public MongoDBStreamSplit(
            String splitId,
            MongoDBChangeStreamConfig config,
            MongoDBChangeStreamOffset offset,
            boolean suspended) {
        super(splitId);
        this.config = Objects.requireNonNull(config);
        this.offset = offset;
        this.suspended = suspended;
    }

    public MongoDBChangeStreamConfig getChangeStreamConfig() {
        return config;
    }

    public MongoDBChangeStreamOffset getChangeStreamOffset() {
        return offset;
    }

    public boolean isSuspended() {
        return suspended;
    }

    public static MongoDBStreamSplit toNormalStreamSplit(
            MongoDBStreamSplit suspendedStreamSplit, MongoDBChangeStreamConfig changeStreamConfig) {
        return new MongoDBStreamSplit(
                suspendedStreamSplit.splitId,
                changeStreamConfig,
                suspendedStreamSplit.offset,
                false);
    }

    public static MongoDBStreamSplit toSuspendedStreamSplit(MongoDBStreamSplit normalStreamSplit) {
        return new MongoDBStreamSplit(
                normalStreamSplit.splitId,
                normalStreamSplit.config,
                normalStreamSplit.offset,
                true);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoDBStreamSplit)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoDBStreamSplit that = (MongoDBStreamSplit) o;
        return suspended == that.suspended
                && Objects.equals(config, that.config)
                && Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), config, offset, suspended);
    }

    @Override
    public String toString() {
        return "MongoDBStreamSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", changeStreamConfig="
                + config
                + ", offset="
                + offset
                + ", isSuspended="
                + suspended
                + '}';
    }
}
