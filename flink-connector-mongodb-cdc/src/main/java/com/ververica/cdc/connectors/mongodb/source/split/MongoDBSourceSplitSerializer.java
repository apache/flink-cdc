/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.split;

import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffsetFactory;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionId;
import com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema;
import org.bson.BsonDocument;

import java.io.IOException;

import static com.ververica.cdc.connectors.mongodb.source.schema.CollectionSchema.SHARDED_KEYS_FIELD;

/** A serializer for the {@link SourceSplitBase}. */
public class MongoDBSourceSplitSerializer
        extends SourceSplitSerializer<CollectionId, CollectionSchema> {

    private final ChangeStreamOffsetFactory offsetFactory;

    public MongoDBSourceSplitSerializer(ChangeStreamOffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public OffsetFactory getOffsetFactory() {
        return offsetFactory;
    }

    @Override
    public CollectionId parseTableId(String identity) {
        return CollectionId.parse(identity);
    }

    @Override
    protected CollectionSchema readTableSchema(String serialized) throws IOException {
        BsonDocument document = BsonDocument.parse(serialized);
        return new CollectionSchema(document.getDocument(SHARDED_KEYS_FIELD));
    }

    @Override
    protected String writeTableSchema(CollectionSchema tableSchema) throws IOException {
        BsonDocument document = new BsonDocument();
        document.put(SHARDED_KEYS_FIELD, tableSchema.getShardKeys());
        return document.toJson();
    }
}
