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

import com.ververica.cdc.connectors.mongodb.source.offset.MongoDBChangeStreamOffset;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import java.util.Objects;

/** The state of split to describe the change stream of MongoDB. */
public class MongoDBStreamSplitState extends MongoDBSplitState {

    private MongoDBChangeStreamOffset offset;

    public MongoDBStreamSplitState(MongoDBStreamSplit streamSplit) {
        super(streamSplit);
        this.offset = streamSplit.getChangeStreamOffset();
    }

    @Override
    public MongoDBSplit toMongoDBSplit() {
        final MongoDBStreamSplit streamSplit = split.asStreamSplit();
        return new MongoDBStreamSplit(
                streamSplit.splitId(),
                streamSplit.getChangeStreamConfig(),
                lastOffset(),
                streamSplit.isSuspended());
    }

    public MongoDBChangeStreamOffset lastOffset() {
        return offset;
    }

    public void updateOffset(BsonDocument resumeToken, BsonTimestamp timestamp) {
        updateOffset(new MongoDBChangeStreamOffset(resumeToken, timestamp));
    }

    public void updateOffset(MongoDBChangeStreamOffset offset) {
        this.offset = Objects.requireNonNull(offset);
    }
}
