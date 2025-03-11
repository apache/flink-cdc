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

package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the copied class {@link TableMapEventDataDeserializer}. */
class TableMapEventDataDeserializerTest {
    @Test
    void testDeserialize() throws IOException {
        TableMapEventDataDeserializer deserializer = new TableMapEventDataDeserializer();
        // The Table_map_event data. See its format at
        // https://dev.mysql.com/doc/dev/mysql-server/latest/classbinary__log_1_1Table__map__event.html
        byte[] data = {
            // table_id : 6 bytes
            1,
            0,
            0,
            0,
            0,
            0,
            // flags : 2 bytes
            1,
            0,
            // database_name string length : 1 byte
            6,
            // database_name null-terminated string, end with 0
            116,
            101,
            115,
            116,
            68,
            98,
            0,
            // table_name string length : 1 byte
            9,
            // table_name null-terminated string, end with 0
            116,
            101,
            115,
            116,
            84,
            97,
            98,
            108,
            101,
            0,
            // column_count
            3,
            // column_type list
            8,
            1,
            20,
            // metadata_length
            1,
            // metadata
            8,
            // null_bits
            80,
            // optional metadata fields stored in Type, Length, Value(TLV) format.
            // Type takes 1 byte. Length is a packed integer value. Values takes Length bytes.

            // SIGNEDNESS
            1,
            1,
            0,
            // DEFAULT_CHARSET
            2,
            1,
            45
        };
        TableMapEventData eventData = deserializer.deserialize(new ByteArrayInputStream(data));
        assertThat(eventData).hasToString(getExpectedEventData().toString());
    }

    @Test
    void testDeserializeMetadata() throws IOException {
        byte[] data = {
            // SIGNEDNESS
            1,
            // SIGNEDNESS length
            5,
            // 5 bytes for SIGNEDNESS
            -74,
            -39,
            -101,
            97,
            0,
            // COLUMN_CHARSET
            3,
            // COLUMN_CHARSET length
            32,
            // remaining 32 bytes for COLUMN_CHARSET
            33,
            33,
            63,
            33,
            63,
            63,
            63,
            63,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0,
            -4,
            -1,
            0
        };

        TableMapEventMetadataDeserializer deserializer = new TableMapEventMetadataDeserializer();

        TableMapEventMetadata metadata =
                deserializer.deserialize(new ByteArrayInputStream(data), 59, 32);

        assertThat(metadata).hasToString(getExpectedTableMapEventMetaData().toString());
    }

    private TableMapEventData getExpectedEventData() {
        TableMapEventData eventData = new TableMapEventData();
        // table_id
        eventData.setTableId(1);
        // database_name
        eventData.setDatabase("testDb");
        // table_name
        eventData.setTable("testTable");

        // column_type
        // 3 column types: MYSQL_TYPE_LONGLONG, MYSQL_TYPE_TINY, MYSQL_TYPE_TYPED_ARRAY<LONGLONG>
        eventData.setColumnTypes(new byte[] {8, 1, 20});

        // metadata of the column types
        eventData.setColumnMetadata(new int[] {0, 0, 0});

        // null_bits
        eventData.setColumnNullability(new BitSet());

        // optional metadata fields
        TableMapEventMetadata metadata = new TableMapEventMetadata();
        metadata.setSignedness(new BitSet());
        TableMapEventMetadata.DefaultCharset charset = new TableMapEventMetadata.DefaultCharset();
        charset.setDefaultCharsetCollation(45);
        metadata.setDefaultCharset(charset);
        eventData.setEventMetadata(metadata);
        return eventData;
    }

    private TableMapEventMetadata getExpectedTableMapEventMetaData() {
        // optional metadata fields
        TableMapEventMetadata metadata = new TableMapEventMetadata();
        BitSet signedness = new BitSet();

        List<Integer> signedBits =
                Arrays.asList(0, 2, 3, 5, 6, 8, 9, 11, 12, 15, 16, 19, 20, 22, 23, 25, 26, 31);

        for (Integer signedBit : signedBits) {
            signedness.set(signedBit);
        }

        metadata.setSignedness(signedness);
        metadata.setColumnCharsets(
                Arrays.asList(
                        33, 33, 63, 33, 63, 63, 63, 63, 255, 255, 255, 255, 255, 255, 255, 255));
        return metadata;
    }
}
