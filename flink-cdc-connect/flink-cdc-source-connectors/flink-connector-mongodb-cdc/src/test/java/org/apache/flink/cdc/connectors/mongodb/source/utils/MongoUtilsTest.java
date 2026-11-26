/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mongodb.source.utils;

import com.mongodb.MongoClientSettings;
import org.assertj.core.api.Assertions;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.NAMESPACE_FIELD;
import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.UUID_FIELD;

/** Unit test cases for {@link MongoUtils}. */
class MongoUtilsTest {

    @Test
    void testChunksFilterUsesNamespaceOnMongo48AndLower() {
        BsonDocument collectionMetadata = collectionMetadata();

        BsonDocument filter = toBsonDocument(MongoUtils.chunksFilter(collectionMetadata, 408));

        Assertions.assertThat(filter)
                .isEqualTo(new BsonDocument(NAMESPACE_FIELD, collectionMetadata.get(ID_FIELD)));
    }

    @Test
    void testChunksFilterUsesUuidOnMongo49AndHigher() {
        BsonDocument collectionMetadata = collectionMetadata();

        BsonDocument filter = toBsonDocument(MongoUtils.chunksFilter(collectionMetadata, 409));

        Assertions.assertThat(filter)
                .isEqualTo(new BsonDocument(UUID_FIELD, collectionMetadata.get(UUID_FIELD)));
    }

    @Test
    void testChunksFilterFallsBackToOrWhenVersionUnknown() {
        BsonDocument collectionMetadata = collectionMetadata();

        BsonDocument filter = toBsonDocument(MongoUtils.chunksFilter(collectionMetadata, null));

        Assertions.assertThat(filter.keySet()).containsExactly("$or");
        Assertions.assertThat(filter.getArray("$or"))
                .isEqualTo(
                        new BsonArray(
                                Arrays.asList(
                                        new BsonDocument(
                                                NAMESPACE_FIELD, collectionMetadata.get(ID_FIELD)),
                                        new BsonDocument(
                                                UUID_FIELD, collectionMetadata.get(UUID_FIELD)))));
    }

    private static BsonDocument toBsonDocument(Bson bson) {
        return bson.toBsonDocument(
                BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
    }

    private static BsonDocument collectionMetadata() {
        return new BsonDocument(ID_FIELD, new BsonString("db.coll"))
                .append(UUID_FIELD, new BsonBinary(new byte[] {1, 2, 3, 4}));
    }
}
