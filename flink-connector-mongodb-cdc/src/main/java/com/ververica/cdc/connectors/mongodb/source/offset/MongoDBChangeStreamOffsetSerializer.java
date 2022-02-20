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

import org.apache.flink.annotation.Internal;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.Optional;

/** Serializer implementation for a {@link MongoDBChangeStreamOffset}. */
@Internal
public class MongoDBChangeStreamOffsetSerializer {

    private static final String RESUME_TOKEN_FIELD = "resumeToken";

    private static final String TIMESTAMP_FIELD = "timestamp";

    private MongoDBChangeStreamOffsetSerializer() {}

    public static String serialize(MongoDBChangeStreamOffset changeStreamOffset) {
        BsonDocument offsetDoc = new BsonDocument();
        Optional.ofNullable(changeStreamOffset.getResumeToken())
                .ifPresent(token -> offsetDoc.put(RESUME_TOKEN_FIELD, token));
        offsetDoc.put(TIMESTAMP_FIELD, changeStreamOffset.getTimestamp());
        return offsetDoc.toJson();
    }

    public static MongoDBChangeStreamOffset deserialize(String serialized) {
        BsonDocument offsetDoc = BsonDocument.parse(serialized);
        BsonDocument resumeToken = offsetDoc.getDocument(RESUME_TOKEN_FIELD, null);
        BsonTimestamp timestamp = offsetDoc.getTimestamp(TIMESTAMP_FIELD);
        return new MongoDBChangeStreamOffset(resumeToken, timestamp);
    }
}
