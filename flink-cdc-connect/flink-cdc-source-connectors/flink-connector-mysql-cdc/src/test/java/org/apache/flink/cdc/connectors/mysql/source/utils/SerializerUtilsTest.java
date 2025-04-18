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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetKind;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Unit test for {@link org.apache.flink.cdc.connectors.mysql.source.utils.SerializerUtils}. */
class SerializerUtilsTest {

    @Test
    void testBinlogOffsetSerde() throws Exception {
        for (BinlogOffset offset : createBinlogOffsets()) {
            byte[] serialized = serializeBinlogOffset(offset);
            BinlogOffset deserialized = deserializeBinlogOffset(serialized);
            Assertions.assertThat(deserialized).isEqualTo(offset);
        }
    }

    /**
     * Test deserializing from old binlog offsets without {@link BinlogOffsetKind}, which validates
     * the backward compatibility.
     */
    @Test
    void testDeserializeFromBinlogOffsetWithoutKind() throws Exception {
        // Create the INITIAL offset in earlier versions
        Map<String, String> initialOffsetMap = new HashMap<>();
        initialOffsetMap.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, "");
        initialOffsetMap.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, "0");
        BinlogOffset initialOffset = new BinlogOffset(initialOffsetMap);
        BinlogOffset deserialized = deserializeBinlogOffset(serializeBinlogOffset(initialOffset));
        Assertions.assertThat(deserialized).isEqualTo(BinlogOffset.ofEarliest());

        // Create the NON_STOPPING offset in earlier versions
        Map<String, String> nonStoppingOffsetMap = new HashMap<>();
        nonStoppingOffsetMap.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, "");
        nonStoppingOffsetMap.put(
                BinlogOffset.BINLOG_POSITION_OFFSET_KEY, Long.toString(Long.MIN_VALUE));
        BinlogOffset nonStoppingOffset = new BinlogOffset(nonStoppingOffsetMap);
        deserialized = deserializeBinlogOffset(serializeBinlogOffset(nonStoppingOffset));
        Assertions.assertThat(deserialized).isEqualTo(BinlogOffset.ofNonStopping());

        // Create a specific offset in earlier versions
        Map<String, String> specificOffsetMap = new HashMap<>();
        specificOffsetMap.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, "mysql-bin.000001");
        specificOffsetMap.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, "4");
        specificOffsetMap.put(
                BinlogOffset.GTID_SET_KEY, "24DA167-0C0C-11E8-8442-00059A3C7B00:1-19");
        specificOffsetMap.put(BinlogOffset.TIMESTAMP_KEY, "1668690384");
        specificOffsetMap.put(BinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY, "15213");
        specificOffsetMap.put(BinlogOffset.ROWS_TO_SKIP_OFFSET_KEY, "18613");
        BinlogOffset specificOffset = new BinlogOffset(specificOffsetMap);
        deserialized = deserializeBinlogOffset(serializeBinlogOffset(specificOffset));
        Assertions.assertThat(deserialized)
                .isEqualTo(
                        BinlogOffset.builder()
                                .setBinlogFilePosition("mysql-bin.000001", 4L)
                                .setGtidSet("24DA167-0C0C-11E8-8442-00059A3C7B00:1-19")
                                .setTimestampSec(1668690384L)
                                .setSkipEvents(15213L)
                                .setSkipRows(18613L)
                                .build());
    }

    private List<BinlogOffset> createBinlogOffsets() {
        return Arrays.asList(
                // Specific offsets
                BinlogOffset.ofBinlogFilePosition("foo-filename", 15213L),
                BinlogOffset.ofGtidSet("foo-gtid"),
                BinlogOffset.ofTimestampSec(15513L),

                // Special offsets
                BinlogOffset.ofNonStopping(),
                BinlogOffset.ofEarliest(),
                BinlogOffset.ofLatest(),

                // Offsets with additional parameters
                BinlogOffset.builder()
                        .setGtidSet("foo-gtid")
                        .setSkipEvents(18213L)
                        .setSkipRows(18613L)
                        .build());
    }

    private byte[] serializeBinlogOffset(BinlogOffset binlogOffset) throws IOException {
        DataOutputSerializer dos = new DataOutputSerializer(64);
        SerializerUtils.writeBinlogPosition(binlogOffset, dos);
        return dos.getCopyOfBuffer();
    }

    private BinlogOffset deserializeBinlogOffset(byte[] serialized) throws IOException {
        DataInputDeserializer did = new DataInputDeserializer(serialized);
        return SerializerUtils.readBinlogPosition(4, did);
    }
}
