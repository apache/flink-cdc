/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.utils;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MAX_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MIN_KEY;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_FIELD;
import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.ID_INDEX;

/** Utilities to split chunks of collection. */
public class ChunkUtils {

    private ChunkUtils() {}

    public static Object[] boundOfId(BsonValue bound) {
        return new Object[] {ID_INDEX, new BsonDocument(ID_FIELD, bound)};
    }

    public static Object[] minLowerBoundOfId() {
        return boundOfId(BSON_MIN_KEY);
    }

    public static Object[] maxUpperBoundOfId() {
        return boundOfId(BSON_MAX_KEY);
    }
}
