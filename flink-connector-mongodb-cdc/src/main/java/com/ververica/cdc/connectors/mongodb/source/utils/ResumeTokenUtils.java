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
import org.bson.BsonTimestamp;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope.DATA_FIELD;

/** Utility class to decode resumeToken. */
public class ResumeTokenUtils {

    private static final int K_TIMESTAMP = 130;

    private ResumeTokenUtils() {}

    public static BsonTimestamp decodeTimestamp(BsonDocument resumeToken) {
        BsonValue bsonValue =
                Objects.requireNonNull(resumeToken, "Missing ResumeToken.").get(DATA_FIELD);
        final byte[] keyStringBytes;
        // Resume Tokens format: https://www.mongodb.com/docs/manual/changeStreams/#resume-tokens
        if (bsonValue.isBinary()) { // BinData
            keyStringBytes = bsonValue.asBinary().getData();
        } else if (bsonValue.isString()) { // Hex-encoded string (v0 or v1)
            keyStringBytes = hexToUint8Array(bsonValue.asString().getValue());
        } else {
            throw new IllegalArgumentException(
                    "Unknown resume token format: " + resumeToken.toJson());
        }

        ByteBuffer buffer = ByteBuffer.wrap(keyStringBytes).order(ByteOrder.BIG_ENDIAN);
        int kType = buffer.get() & 0xff;
        if (kType != K_TIMESTAMP) {
            throw new IllegalArgumentException("Unknown keyType of timestamp: " + kType);
        }

        int t = buffer.getInt();
        int i = buffer.getInt();
        return new BsonTimestamp(t, i);
    }

    private static byte[] hexToUint8Array(String str) {
        int len = str.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] =
                    (byte)
                            ((Character.digit(str.charAt(i), 16) << 4)
                                    + Character.digit(str.charAt(i + 1), 16));
        }
        return data;
    }
}
