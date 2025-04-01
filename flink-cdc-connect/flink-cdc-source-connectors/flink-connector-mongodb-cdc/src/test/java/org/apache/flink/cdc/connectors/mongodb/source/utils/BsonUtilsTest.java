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

import org.assertj.core.api.Assertions;
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

/** Unit test cases for <code>BsonUtils</code> utility class. */
class BsonUtilsTest {

    @Test
    void testCompareBsonValue() {
        // test compare Decimal128
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("18")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isPositive();
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("17")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isZero();
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("16")),
                                new BsonDecimal128(Decimal128.parse("17"))))
                .isNegative();

        // test compare String
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonString("apple"), new BsonString("banana")))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonString("banana"), new BsonString("banana")))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonString("cherry"), new BsonString("banana")))
                .isPositive();

        // test compare Array
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"),
                                                new BsonString("banana")))))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("banana"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"),
                                                new BsonString("banana")))))
                .isZero();

        Assertions.assertThat(
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
        Assertions.assertThat(
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
        Assertions.assertThat(
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
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"),
                                                new BsonString("banana")))))
                .isNegative();

        // test compare Binary
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("apple".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("banana".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonBinary("cherry".getBytes()),
                                new BsonBinary("banana".getBytes())))
                .isPositive();

        // test compare Boolean
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(new BsonBoolean(false), new BsonBoolean(true)))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(true)))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(false)))
                .isPositive();

        // test compare DateTime
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1600000000), new BsonDateTime(1700000000)))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1700000000), new BsonDateTime(1700000000)))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDateTime(1800000000), new BsonDateTime(1700000000)))
                .isPositive();

        // test compare document
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("apple")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("banana")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("cherry")),
                                new BsonDocument("fruit", new BsonString("banana"))))
                .isPositive();

        // test compare Timestamp
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1600000000), new BsonTimestamp(1700000000)))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1700000000), new BsonTimestamp(1700000000)))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonTimestamp(1800000000), new BsonTimestamp(1700000000)))
                .isPositive();

        // test compare RegEx
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[a-xA-X]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[b-yB-Y]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[c-zC-Z]"),
                                new BsonRegularExpression("[b-yB-Y]")))
                .isPositive();

        // test compare JavaScript
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('cherry');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument())))
                .isPositive();

        // test compare JavaScript with different scope
        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("apple"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isNegative();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isZero();

        Assertions.assertThat(
                        BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("cherry"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana")))))
                .isPositive();

        // test inter-type comparison
        Assertions.assertThat(BsonUtils.compareBsonValue(new BsonNull(), new BsonString("")))
                .isNegative();

        Assertions.assertThat(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonString("")))
                .isPositive();

        // test null comparison
        Assertions.assertThat(BsonUtils.compareBsonValue(new BsonNull(), new BsonNull())).isZero();
        Assertions.assertThat(BsonUtils.compareBsonValue(new BsonUndefined(), new BsonUndefined()))
                .isZero();
        Assertions.assertThat(BsonUtils.compareBsonValue(new BsonUndefined(), new BsonNull()))
                .isNegative();
    }
}
