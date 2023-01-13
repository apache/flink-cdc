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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utility class to deal {@link BsonValue}s. */
public class BsonUtils {

    private BsonUtils() {}

    public static int compareBsonValue(BsonValue o1, BsonValue o2) {
        return compareBsonValue(o1, o2, true);
    }

    private static int compareBsonValue(BsonValue o1, BsonValue o2, boolean isTopLevel) {
        if (isTopLevel) {
            BsonValue element1 = o1;
            BsonValue element2 = o2;
            // With top level comparison, we should extract the smallest value from the array
            if (isArray(o1)) {
                element1 = smallestValueOfArray(o1.asArray());
            }
            if (isArray(o2)) {
                element2 = smallestValueOfArray(o2.asArray());
            }
            return compareBsonValue(element1, element2, false);
        }

        // Different type: respect to type order
        if (typeOrder(o1) != typeOrder(o2)) {
            return Integer.compare(typeOrder(o1), typeOrder(o2));
        }

        if (isNull(o1) || isMinKey(o1) || isMaxKey(o1)) {
            return 0; // Null == Null, MinKey == MinKey, MaxKey == MaxKey
        }
        if (isBsonNumber(o1)) {
            return toDecimal128(o1).compareTo(toDecimal128(o2)); // Number compare
        }
        if (o1.isString() || o1.isSymbol()) {
            return toJavaString(o1).compareTo(toJavaString(o2)); // String compare
        }
        if (o1.isDocument() || o1.isDBPointer()) { // Object compare
            return compareBsonDocument(toBsonDocument(o1), toBsonDocument(o2));
        }
        if (o1.isArray()) {
            return compareBsonArray(o1.asArray(), o2.asArray()); // Array compare
        }
        if (o1.isBinary()) {
            return compareBsonBinary(o1.asBinary(), o2.asBinary()); // Binary compare
        }
        if (o1.isObjectId()) {
            return o1.asObjectId().compareTo(o2.asObjectId()); // ObjectId compare
        }
        if (o1.isBoolean()) {
            return o1.asBoolean().compareTo(o2.asBoolean()); // Booleans compare
        }
        if (o1.isDateTime()) {
            return o1.asDateTime().compareTo(o2.asDateTime()); // DateTime compare
        }
        if (o1.isTimestamp()) {
            return o1.asTimestamp().compareTo(o2.asTimestamp()); // Timestamp compare
        }
        if (o1.isRegularExpression() || o1.isJavaScript()) {
            return toJavaString(o1).compareTo(toJavaString(o2)); // String compare
        }
        if (o1.isJavaScriptWithScope()) {
            return compareJavascriptWithScope(
                    o1.asJavaScriptWithScope(), o2.asJavaScriptWithScope()); // Code compare
        }

        throw new IllegalArgumentException(
                String.format("Unable to compare bson values between %s and %s", o1, o2));
    }

    public static int compareBsonDocument(BsonDocument d1, BsonDocument d2) {
        List<Map.Entry<String, BsonValue>> pairs1 = new ArrayList<>(d1.entrySet());
        List<Map.Entry<String, BsonValue>> pairs2 = new ArrayList<>(d2.entrySet());

        if (pairs1.isEmpty() && pairs2.isEmpty()) {
            return 0;
        } else if (pairs1.isEmpty()) {
            return -1;
        } else if (pairs2.isEmpty()) {
            return 1;
        } else {
            // 1. Recursively compare key-value pairs in the order that they appear within the BSON
            // object.
            int minLength = Math.min(pairs1.size(), pairs2.size());
            int result;
            String k1, k2;
            BsonValue v1, v2;
            for (int i = 0; i < minLength; i++) {
                k1 = pairs1.get(i).getKey();
                k2 = pairs2.get(i).getKey();
                v1 = pairs1.get(i).getValue();
                v2 = pairs2.get(i).getValue();

                // 2. Compare the field types.
                result = Integer.compare(typeOrder(v1), typeOrder(v2));
                if (result != 0) {
                    return result;
                }

                // 3. If the field types are equal, compare the key field names.
                result = k1.compareTo(k2);
                if (result != 0) {
                    return result;
                }

                // 4. If the key field names are equal, compare the field values.
                result = compareBsonValue(v1, v2, false);
                // Compare the field types.
                if (result != 0) {
                    return result;
                }

                // 5. If the field values are equal, compare the next key/value pair (return to step
                // 1).
            }

            // An object without further pairs is less than an object with further pairs.
            return Integer.compare(pairs1.size(), pairs2.size());
        }
    }

    public static int compareBsonArray(BsonArray a1, BsonArray a2) {
        return compareBsonValue(smallestValueOfArray(a1), smallestValueOfArray(a2), false);
    }

    private static BsonValue smallestValueOfArray(BsonArray bsonArray) {
        // Treats the empty array as less than null or a missing field.
        if (bsonArray.isEmpty()) {
            return new BsonUndefined();
        }
        // When comparing a field whose value is a one element array (example, [ 1 ]) with non-array
        // fields (example, 2), the comparison is for 1 and 2.
        if (bsonArray.size() == 1) {
            return bsonArray.get(0);
        }
        // A less-than comparison, or an ascending sort, compares the smallest elements of the array
        // according to the BSON type sort order.
        List<BsonValue> sortedValues =
                bsonArray.getValues().stream()
                        .sorted((e1, e2) -> compareBsonValue(e1, e2, false))
                        .collect(Collectors.toList());

        return sortedValues.get(0);
    }

    public static int compareBsonBinary(BsonBinary b1, BsonBinary b2) {
        // First, the length or size of the data.
        byte[] data1 = b1.getData();
        byte[] data2 = b2.getData();

        int result = Integer.compare(data1.length, data2.length);
        if (result != 0) {
            return result;
        }
        // Then, by the BSON one-byte subtype.
        result = Byte.compare(b1.getType(), b2.getType());
        if (result != 0) {
            return result;
        }
        // Finally, by the data, performing a byte-by-byte comparison.
        for (int i = 0; i < data1.length; i++) {
            if (data1[i] != data2[i]) {
                // unsigned
                return Integer.compare(data1[i] & 0xff, data2[i] & 0xff);
            }
        }
        return 0;
    }

    public static int compareJavascriptWithScope(
            BsonJavaScriptWithScope c1, BsonJavaScriptWithScope c2) {
        int result = c1.getCode().compareTo(c2.getCode());
        if (result != 0) {
            return result;
        }
        return compareBsonDocument(c1.getScope(), c2.getScope());
    }

    public static boolean isNull(BsonValue bsonValue) {
        return bsonValue == null
                || bsonValue.isNull()
                || bsonValue.getBsonType() == BsonType.UNDEFINED;
    }

    public static boolean isBsonNumber(BsonValue bsonValue) {
        return bsonValue != null && (bsonValue.isNumber() || bsonValue.isDecimal128());
    }

    public static boolean isArray(BsonValue bsonValue) {
        return bsonValue != null && bsonValue.isArray();
    }

    public static boolean isMinKey(BsonValue bsonValue) {
        return bsonValue != null && bsonValue.getBsonType() == BsonType.MIN_KEY;
    }

    public static boolean isMaxKey(BsonValue bsonValue) {
        return bsonValue != null && bsonValue.getBsonType() == BsonType.MAX_KEY;
    }

    public static Decimal128 toDecimal128(BsonValue bsonValue) {
        if (bsonValue.isNumber()) {
            return bsonValue.asNumber().decimal128Value();
        } else if (bsonValue.isDecimal128()) {
            return bsonValue.asDecimal128().decimal128Value();
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert to Decimal128 with unexpected value: " + bsonValue);
        }
    }

    public static String toJavaString(BsonValue bsonValue) {
        if (bsonValue.isString()) {
            return bsonValue.asString().toString();
        } else if (bsonValue.isSymbol()) {
            return bsonValue.asSymbol().getSymbol();
        } else if (bsonValue.isRegularExpression()) {
            BsonRegularExpression regex = bsonValue.asRegularExpression();
            return String.format("/%s/%s", regex.getPattern(), regex.getOptions());
        } else if (bsonValue.isJavaScript()) {
            return bsonValue.asJavaScript().getCode();
        } else if (bsonValue.isJavaScriptWithScope()) {
            return bsonValue.asJavaScriptWithScope().getCode();
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert to String with unexpected value: " + bsonValue);
        }
    }

    public static BsonDocument toBsonDocument(BsonValue bsonValue) {
        if (bsonValue.isDocument()) {
            return bsonValue.asDocument();
        } else if (bsonValue.isDBPointer()) {
            BsonDbPointer dbPointer = bsonValue.asDBPointer();
            return new BsonDocument("$ref", new BsonString(dbPointer.getNamespace()))
                    .append("$id", new BsonObjectId(dbPointer.getId()));
        } else {
            throw new IllegalArgumentException(
                    "Cannot convert to Document with unexpected value: " + bsonValue);
        }
    }

    public static int typeOrder(BsonValue bsonValue) {
        // Missing Key field
        if (bsonValue == null) {
            return 3;
        }

        switch (bsonValue.getBsonType()) {
                // MinKey < Undefined == [] < Null, Missing Key
            case MIN_KEY:
                return 1;
            case UNDEFINED:
                return 2;
            case NULL:
                return 3;
            case INT32:
            case INT64:
            case DOUBLE:
            case DECIMAL128:
                return 4;
            case STRING:
            case SYMBOL:
                return 5;
            case DOCUMENT:
            case DB_POINTER:
                return 6;
            case ARRAY:
                return 7;
            case BINARY:
                return 8;
            case OBJECT_ID:
                return 9;
            case BOOLEAN:
                return 10;
            case DATE_TIME:
                return 11;
            case TIMESTAMP:
                return 12;
            case REGULAR_EXPRESSION:
                return 13;
            case JAVASCRIPT:
                return 14;
            case JAVASCRIPT_WITH_SCOPE:
                return 15;
            case MAX_KEY:
                return 99;
            default:
                throw new IllegalArgumentException(
                        "Unknown bson type : " + bsonValue.getBsonType());
        }
    }
}
