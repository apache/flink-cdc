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

package com.ververica.cdc.connectors.mongodb.utils;

import com.jayway.jsonpath.JsonPath;
import com.mongodb.client.model.changestream.OperationType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** MongoDB test assert utils. */
public class MongoDBAssertUtils {

    public static void assertObjectIdEquals(String expect, SourceRecord actual) {
        String actualOid = ((Struct) actual.value()).getString("documentKey");
        assertEquals(expect, JsonPath.read(actualOid, "$._id.$oid"));
    }

    public static void assertInsert(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            assertNotNull(record.key());
            assertNotNull(record.keySchema());
        } else {
            assertNull(record.key());
            assertNull(record.keySchema());
        }

        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(OperationType.INSERT.getValue(), value.getString("operationType"));
        assertNotNull(value.get("fullDocument"));
        assertNotNull(value.get("documentKey"));
    }

    public static void assertUpdate(SourceRecord record) {
        assertNotNull(record.key());
        assertNotNull(record.keySchema());

        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(OperationType.UPDATE.getValue(), value.getString("operationType"));
        assertNotNull(value.get("documentKey"));
        assertNotNull(value.get("fullDocument"));
    }

    public static void assertUpdate(SourceRecord record, String idValue) {
        assertUpdate(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertReplace(SourceRecord record) {
        assertNotNull(record.key());
        assertNotNull(record.keySchema());

        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(OperationType.REPLACE.getValue(), value.getString("operationType"));
        assertNotNull(value.get("documentKey"));
        assertNotNull(value.get("fullDocument"));
    }

    public static void assertReplace(SourceRecord record, String idValue) {
        assertReplace(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertDelete(SourceRecord record) {
        assertNotNull(record.key());
        assertNotNull(record.keySchema());

        assertNotNull(record.valueSchema());
        Struct value = (Struct) record.value();
        assertNotNull(value);
        assertEquals(OperationType.DELETE.getValue(), value.getString("operationType"));
        assertNotNull(value.get("documentKey"));
    }

    public static void assertDelete(SourceRecord record, String idValue) {
        assertDelete(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
