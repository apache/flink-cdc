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

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test cases for <code>BsonUtils</code> utility class. */
class BsonUtilsTest {

    @Test
    void testCompareBsonDecimal128Value() {
        // test compare Decimal128
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("16")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("18")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isPositive();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("17")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isZero();
    }

    @Test
    void testCompareBsonStringValue() {
        // test compare String
        assertThat(BsonUtils.compareBsonValue(new BsonString("apple"), new BsonString("banana")))
                .isNegative();
        assertThat(BsonUtils.compareBsonValue(new BsonString("banana"), new BsonString("banana")))
                .isZero();
        assertThat(BsonUtils.compareBsonValue(new BsonString("cherry"), new BsonString("banana")))
                .isPositive();
    }

    @Test
    void testCompareBsonArrayValue() {
        // test compare Array
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"),
                                                new BsonString("banana")))))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("banana"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"),
                                                new BsonString("banana")))))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("cherry"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"),
                                                new BsonString("banana")))))
                .isPositive();

        // According to https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
        // only smallest value will be compared
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("apple"), new BsonString("cherry"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("apple"),
                                                new BsonString("banana")))))
                .isZero();

        // all arrays will be sorted before comparison
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("apple"), new BsonString("banana"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("banana"),
                                                new BsonString("apple")))))
                .isZero();

        // only smallest value in each array will be compared
        // in this case, apple < banana
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"),
                                                new BsonString("banana")))))
                .isNegative();
    }

    @Test
    void testCompareBsonBinaryValue() {
        // test compare Binary
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("apple".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("banana".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("cherry".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isPositive();
    }

    @Test
    void testCompareBsonBooleanValue() {
        // test compare Boolean
        assertThat(BsonUtils.compareBsonValue(new BsonBoolean(false), new BsonBoolean(true)))
                .isNegative();
        assertThat(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(true)))
                .isZero();
        assertThat(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(false)))
                .isPositive();
    }

    @Test
    void testCompareBsonDateTimeValue() {
        // test compare DateTime
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1600000000), new BsonDateTime(1700000000)))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1700000000), new BsonDateTime(1700000000)))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1800000000), new BsonDateTime(1700000000)))
                .isPositive();
    }

    @Test
    void testCompareBsonDocumentValue() {
        // test compare document
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("apple")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isNegative();

        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("banana")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isZero();

        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("cherry")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isPositive();
    }

    @Test
    void testCompareBsonTimestampValue() {
        // test compare Timestamp
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1600000000), new BsonTimestamp(1700000000)))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1700000000), new BsonTimestamp(1700000000)))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1800000000), new BsonTimestamp(1700000000)))
                .isPositive();
    }

    @Test
    void testCompareBsonValue() {
        // test compare RegEx
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[a-xA-X]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[b-yB-Y]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[c-zC-Z]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isPositive();
    }

    @Test
    void testCompareBsonJavaScriptValue() {
        // test compare JavaScript
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('cherry');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isPositive();
    }

    @Test
    void testCompareBsonJavaScriptWithScopeValue() {
        // test compare JavaScript with different scope
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("apple"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isNegative();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isZero();
        assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("cherry"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isPositive();
    }

    @Test
    void testCompareBsonInterTypeValue() {
        // test inter-type comparison
        assertThat(BsonUtils.compareBsonValue(new BsonNull(), new BsonString(""))).isNegative();
        assertThat(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonString("")))
                .isPositive();
    }

    @Test
    void testCompareBsonNullValue() {
        // test null comparison
        assertThat(BsonUtils.compareBsonValue(new BsonNull(), new BsonNull())).isZero();
        assertThat(BsonUtils.compareBsonValue(new BsonUndefined(), new BsonUndefined())).isZero();
        assertThat(BsonUtils.compareBsonValue(new BsonUndefined(), new BsonNull())).isNegative();
    }
}
