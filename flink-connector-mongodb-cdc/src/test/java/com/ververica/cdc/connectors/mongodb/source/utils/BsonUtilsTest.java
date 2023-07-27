/*
 * Copyright 2023 Ververica Inc.
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
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

/** Unit test cases for <code>BsonUtils</code> utility class. */
public class BsonUtilsTest {

    @Test
    public void testCompareBsonValue() {
        // test compare Decimal128
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("18")),
                                new BsonDecimal128(Decimal128.parse("17")))
                        > 0);
        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonDecimal128(Decimal128.parse("17")),
                        new BsonDecimal128(Decimal128.parse("17"))));
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDecimal128(Decimal128.parse("16")),
                                new BsonDecimal128(Decimal128.parse("17")))
                        < 0);

        // test compare String
        assertTrue(
                BsonUtils.compareBsonValue(new BsonString("apple"), new BsonString("banana")) < 0);

        assertEquals(
                0, BsonUtils.compareBsonValue(new BsonString("banana"), new BsonString("banana")));

        assertTrue(
                BsonUtils.compareBsonValue(new BsonString("cherry"), new BsonString("banana")) > 0);

        // test compare Array
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("banana"))))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonArray(
                                Arrays.asList(new BsonString("fruit"), new BsonString("banana"))),
                        new BsonArray(
                                Arrays.asList(new BsonString("fruit"), new BsonString("banana")))));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("cherry"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("fruit"), new BsonString("banana"))))
                        > 0);

        // According to https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
        // only smallest value will be compared
        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonArray(
                                Arrays.asList(new BsonString("apple"), new BsonString("cherry"))),
                        new BsonArray(
                                Arrays.asList(new BsonString("apple"), new BsonString("banana")))));

        // all arrays will be sorted before comparison
        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonArray(
                                Arrays.asList(new BsonString("apple"), new BsonString("banana"))),
                        new BsonArray(
                                Arrays.asList(new BsonString("banana"), new BsonString("apple")))));

        // only smallest value in each array will be compared
        // in this case, apple < banana
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"), new BsonString("apple"))),
                                new BsonArray(
                                        Arrays.asList(
                                                new BsonString("cherry"),
                                                new BsonString("banana"))))
                        < 0);

        // test compare Binary
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonBinary("apple".getBytes()),
                                new BsonBinary("banana".getBytes()))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonBinary("banana".getBytes()), new BsonBinary("banana".getBytes())));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonBinary("cherry".getBytes()),
                                new BsonBinary("banana".getBytes()))
                        > 0);

        // test compare Boolean
        assertTrue(BsonUtils.compareBsonValue(new BsonBoolean(false), new BsonBoolean(true)) < 0);

        assertEquals(0, BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(true)));

        assertTrue(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonBoolean(false)) > 0);

        // test compare DateTime
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDateTime(1600000000), new BsonDateTime(1700000000))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonDateTime(1700000000), new BsonDateTime(1700000000)));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDateTime(1800000000), new BsonDateTime(1700000000))
                        > 0);

        // test compare document
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("apple")),
                                new BsonDocument("fruit", new BsonString("banana")))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonDocument("fruit", new BsonString("banana")),
                        new BsonDocument("fruit", new BsonString("banana"))));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonDocument("fruit", new BsonString("cherry")),
                                new BsonDocument("fruit", new BsonString("banana")))
                        > 0);

        // test compare Timestamp
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonTimestamp(1600000000), new BsonTimestamp(1700000000))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonTimestamp(1700000000), new BsonTimestamp(1700000000)));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonTimestamp(1800000000), new BsonTimestamp(1700000000))
                        > 0);

        // test compare RegEx
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[a-xA-X]"),
                                new BsonRegularExpression("[b-yB-Y]"))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonRegularExpression("[b-yB-Y]"),
                        new BsonRegularExpression("[b-yB-Y]")));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonRegularExpression("[c-zC-Z]"),
                                new BsonRegularExpression("[b-yB-Y]"))
                        > 0);

        // test compare JavaScript
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument()))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonJavaScriptWithScope("console.log('banana');", new BsonDocument()),
                        new BsonJavaScriptWithScope("console.log('banana');", new BsonDocument())));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('cherry');", new BsonDocument()),
                                new BsonJavaScriptWithScope(
                                        "console.log('banana');", new BsonDocument()))
                        > 0);

        // test compare JavaScript with different scope
        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("apple"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana"))))
                        < 0);

        assertEquals(
                0,
                BsonUtils.compareBsonValue(
                        new BsonJavaScriptWithScope(
                                "console.log('apple');",
                                new BsonDocument("_id", new BsonString("banana"))),
                        new BsonJavaScriptWithScope(
                                "console.log('apple');",
                                new BsonDocument("_id", new BsonString("banana")))));

        assertTrue(
                BsonUtils.compareBsonValue(
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("cherry"))),
                                new BsonJavaScriptWithScope(
                                        "console.log('apple');",
                                        new BsonDocument("_id", new BsonString("banana"))))
                        > 0);

        // test inter-type comparison
        assertTrue(BsonUtils.compareBsonValue(new BsonNull(), new BsonString("")) < 0);

        assertTrue(BsonUtils.compareBsonValue(new BsonBoolean(true), new BsonString("")) > 0);

        // test null comparison
        assertEquals(0, BsonUtils.compareBsonValue(new BsonNull(), new BsonNull()));
        assertEquals(0, BsonUtils.compareBsonValue(new BsonUndefined(), new BsonUndefined()));
        assertTrue(BsonUtils.compareBsonValue(new BsonUndefined(), new BsonNull()) < 0);
    }
}
