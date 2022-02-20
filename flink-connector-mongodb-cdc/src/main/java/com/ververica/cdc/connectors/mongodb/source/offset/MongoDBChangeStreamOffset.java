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

package com.ververica.cdc.connectors.mongodb.source.offset;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * A structure describes a fine grained offset in a change log event including resumeToken and
 * clusterTime.
 */
public class MongoDBChangeStreamOffset implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final BsonDocument resumeToken;

    private final BsonTimestamp timestamp;

    public MongoDBChangeStreamOffset(@Nullable BsonDocument resumeToken, BsonTimestamp timestamp) {
        this.resumeToken = resumeToken;
        this.timestamp = Objects.requireNonNull(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MongoDBChangeStreamOffset)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MongoDBChangeStreamOffset that = (MongoDBChangeStreamOffset) o;
        return Objects.equals(resumeToken, that.resumeToken)
                && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resumeToken, timestamp);
    }

    @Override
    public String toString() {
        return "MongoDBChangeStreamOffset{"
                + "timestamp="
                + timestamp
                + ", resumeToken="
                + resumeToken
                + '}';
    }

    @Nullable
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    public BsonTimestamp getTimestamp() {
        return timestamp;
    }
}
