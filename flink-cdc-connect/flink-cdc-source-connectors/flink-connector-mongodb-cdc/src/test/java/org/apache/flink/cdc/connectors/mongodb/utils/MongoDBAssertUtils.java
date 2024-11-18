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

package org.apache.flink.cdc.connectors.mongodb.utils;

import com.jayway.jsonpath.JsonPath;
import com.mongodb.client.model.changestream.OperationType;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;

import java.util.List;

/** MongoDB test assert utils. */
public class MongoDBAssertUtils {

    public static void assertObjectIdEquals(String expect, SourceRecord actual) {
        String actualOid = ((Struct) actual.value()).getString("documentKey");
        Assertions.assertThat(JsonPath.<String>read(actualOid, "$._id.$oid")).isEqualTo(expect);
    }

    public static void assertInsert(SourceRecord record, boolean keyExpected) {
        if (keyExpected) {
            Assertions.assertThat(record.key()).isNotNull();
            Assertions.assertThat(record.keySchema()).isNotNull();
        } else {
            Assertions.assertThat(record.key()).isNull();
            Assertions.assertThat(record.keySchema()).isNull();
        }

        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString("operationType"))
                .isEqualTo(OperationType.INSERT.getValue());
        Assertions.assertThat(value.get("fullDocument")).isNotNull();
        Assertions.assertThat(value.get("documentKey")).isNotNull();
    }

    public static void assertUpdate(SourceRecord record) {
        Assertions.assertThat(record.key()).isNotNull();
        Assertions.assertThat(record.keySchema()).isNotNull();

        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString("operationType"))
                .isEqualTo(OperationType.UPDATE.getValue());
        Assertions.assertThat(value.get("documentKey")).isNotNull();
        Assertions.assertThat(value.get("fullDocument")).isNotNull();
    }

    public static void assertUpdate(SourceRecord record, String idValue) {
        assertUpdate(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertReplace(SourceRecord record) {
        Assertions.assertThat(record.key()).isNotNull();
        Assertions.assertThat(record.keySchema()).isNotNull();

        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString("operationType"))
                .isEqualTo(OperationType.REPLACE.getValue());
        Assertions.assertThat(value.get("documentKey")).isNotNull();
        Assertions.assertThat(value.get("fullDocument")).isNotNull();
    }

    public static void assertReplace(SourceRecord record, String idValue) {
        assertReplace(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertDelete(SourceRecord record) {
        Assertions.assertThat(record.key()).isNotNull();
        Assertions.assertThat(record.keySchema()).isNotNull();

        Assertions.assertThat(record.valueSchema()).isNotNull();
        Struct value = (Struct) record.value();
        Assertions.assertThat(value).isNotNull();
        Assertions.assertThat(value.getString("operationType"))
                .isEqualTo(OperationType.DELETE.getValue());
        Assertions.assertThat(value.get("documentKey")).isNotNull();
    }

    public static void assertDelete(SourceRecord record, String idValue) {
        assertDelete(record);
        assertObjectIdEquals(idValue, record);
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }
}
