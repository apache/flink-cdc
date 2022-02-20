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

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * A structure describes a fine grained offset in a change log event including resumeToken and
 * clusterTime.
 */
public class ChangeStreamOffset extends Offset {

    public static final String DATABASE_FIELD = "database";

    public static final String COLLECTION_FIELD = "collection";

    public static final String DATABASE_REGEX_FIELD = "databaseRegex";

    public static final String NAMESPACE_REGEX_FIELD = "namespaceRegex";

    public static final String TIMESTAMP_FIELD = "timestamp";

    public static final String RESUME_TOKEN_FIELD = "resumeToken";

    public ChangeStreamOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public ChangeStreamOffset(
            ChangeStreamDescriptor descriptor,
            @Nullable BsonDocument resumeToken,
            BsonTimestamp timestamp) {
        this(
                descriptor.getDatabase(),
                descriptor.getCollection(),
                descriptor.getDatabaseRegex(),
                descriptor.getNamespaceRegex(),
                resumeToken,
                timestamp);
    }

    public ChangeStreamOffset(
            @Nullable String database,
            @Nullable String collection,
            @Nullable Pattern databaseRegex,
            @Nullable Pattern collectionRegex,
            @Nullable BsonDocument resumeToken,
            BsonTimestamp timestamp) {
        Objects.requireNonNull(timestamp);
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(DATABASE_FIELD, database);
        offsetMap.put(COLLECTION_FIELD, collection);
        offsetMap.put(
                DATABASE_REGEX_FIELD,
                Optional.ofNullable(databaseRegex).map(Pattern::pattern).orElse(null));
        offsetMap.put(
                NAMESPACE_REGEX_FIELD,
                Optional.ofNullable(collectionRegex).map(Pattern::pattern).orElse(null));
        offsetMap.put(TIMESTAMP_FIELD, String.valueOf(timestamp.getValue()));
        offsetMap.put(
                RESUME_TOKEN_FIELD,
                Optional.ofNullable(resumeToken).map(BsonDocument::toJson).orElse(null));
        this.offset = offsetMap;
    }

    public void updatePosition(@Nullable BsonDocument resumeToken, BsonTimestamp timestamp) {
        offset.put(TIMESTAMP_FIELD, String.valueOf(timestamp.getValue()));
        offset.put(
                RESUME_TOKEN_FIELD,
                Optional.ofNullable(resumeToken).map(BsonDocument::toJson).orElse(null));
    }

    @Nullable
    public String getDatabase() {
        return offset.get(DATABASE_FIELD);
    }

    @Nullable
    public String getCollection() {
        return offset.get(COLLECTION_FIELD);
    }

    @Nullable
    public Pattern getDatabaseRegex() {
        return Optional.ofNullable(offset.get(DATABASE_REGEX_FIELD))
                .map(Pattern::compile)
                .orElse(null);
    }

    @Nullable
    public Pattern getNamespaceRegex() {
        return Optional.ofNullable(offset.get(NAMESPACE_REGEX_FIELD))
                .map(Pattern::compile)
                .orElse(null);
    }

    @Nullable
    public BsonDocument getResumeToken() {
        String resumeTokenJson = offset.get(RESUME_TOKEN_FIELD);
        return Optional.ofNullable(resumeTokenJson).map(BsonDocument::parse).orElse(null);
    }

    public BsonTimestamp getTimestamp() {
        long timestamp = Long.parseLong(offset.get(TIMESTAMP_FIELD));
        return new BsonTimestamp(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChangeStreamOffset)) {
            return false;
        }
        ChangeStreamOffset that = (ChangeStreamOffset) o;
        return offset.equals(that.offset);
    }

    @Override
    public int compareTo(Offset offset) {
        if (offset == null) {
            return -1;
        }
        ChangeStreamOffset that = (ChangeStreamOffset) offset;
        return this.getTimestamp().compareTo(that.getTimestamp());
    }
}
