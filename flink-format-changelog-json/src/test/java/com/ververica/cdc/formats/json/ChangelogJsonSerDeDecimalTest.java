/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.formats.json;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ChangelogJsonSerializationSchema} and {@link
 * ChangelogJsonDeserializationSchema}.
 */
public class ChangelogJsonSerDeDecimalTest {

    private static final RowType SCHEMA =
            (RowType)
                    ROW(
                                    FIELD("id", INT().notNull()),
                                    FIELD("name", STRING()),
                                    FIELD("money", DECIMAL(20, 2)))
                            .getLogicalType();

    @Test
    public void testDecimalSerializationDeserialization() throws Exception {
        List<String> lines = readLines("changelog-json-data2.txt");
        ChangelogJsonDeserializationSchema deserializationSchema =
                new ChangelogJsonDeserializationSchema(
                        SCHEMA, InternalTypeInfo.of(SCHEMA), false, TimestampFormat.SQL);

        deserializationSchema.open(null);
        SimpleCollector collector = new SimpleCollector();
        for (String line : lines) {
            deserializationSchema.deserialize(line.getBytes(StandardCharsets.UTF_8), collector);
        }

        List<String> expected =
                Arrays.asList(
                        "+I(112,liaooo,10.00)",
                        "-U(112,liaooo,10.00)",
                        "+U(112,liaooo,1000000.00)",
                        "-U(112,liaooo,1000000.00)",
                        "+U(112,liaooo,9999999.50)");
        List<String> actual =
                collector.list.stream().map(Object::toString).collect(Collectors.toList());
        assertEquals(expected, actual);

        Configuration formatOptions = new Configuration();
        formatOptions.setString(JsonOptions.TIMESTAMP_FORMAT.key(), JsonOptions.SQL);
        formatOptions.setBoolean(JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER.key(), false);

        // decimal字段会启用科学记数法
        ChangelogJsonSerializationSchema serializationSchema =
                new ChangelogJsonSerializationSchema(SCHEMA, formatOptions);
        serializationSchema.open(null);
        List<String> result = new ArrayList<>();
        for (RowData rowData : collector.list) {
            result.add(new String(serializationSchema.serialize(rowData), StandardCharsets.UTF_8));
        }
        List<String> expectedResult =
                Arrays.asList(
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1E+1},\"op\":\"+I\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1E+1},\"op\":\"-U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1E+6},\"op\":\"+U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1E+6},\"op\":\"-U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":9999999.5},\"op\":\"+U\"}");
        assertEquals(expectedResult, result);

        // 使decimal字段以plain形式展示
        formatOptions.setBoolean(JsonOptions.ENCODE_DECIMAL_AS_PLAIN_NUMBER.key(), true);
        ChangelogJsonSerializationSchema serializationSchema2 =
                new ChangelogJsonSerializationSchema(SCHEMA, formatOptions);
        serializationSchema2.open(null);
        List<String> result2 = new ArrayList<>();
        for (RowData rowData : collector.list) {
            result2.add(
                    new String(serializationSchema2.serialize(rowData), StandardCharsets.UTF_8));
        }
        List<String> expectedResult2 =
                Arrays.asList(
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":10},\"op\":\"+I\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":10},\"op\":\"-U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1000000},\"op\":\"+U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":1000000},\"op\":\"-U\"}",
                        "{\"data\":{\"id\":112,\"name\":\"liaooo\",\"money\":9999999.5},\"op\":\"+U\"}");
        assertEquals(expectedResult2, result2);
    }

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    private static List<String> readLines(String resource) throws IOException {
        final URL url = ChangelogJsonSerDeDecimalTest.class.getClassLoader().getResource(resource);
        assert url != null;
        Path path = new File(url.getFile()).toPath();
        return Files.readAllLines(path);
    }

    private static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
