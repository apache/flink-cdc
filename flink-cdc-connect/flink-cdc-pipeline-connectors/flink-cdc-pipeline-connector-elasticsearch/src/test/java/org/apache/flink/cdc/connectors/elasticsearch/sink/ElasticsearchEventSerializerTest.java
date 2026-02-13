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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.ZonedTimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.elasticsearch.serializer.ElasticsearchEventSerializer;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ElasticsearchEventSerializer}. */
public class ElasticsearchEventSerializerTest {
    TableId tableId = TableId.tableId("test");

    @Test
    void testTableShardingWithString() {
        HashMap<TableId, String> shardingKey = new HashMap<>();
        shardingKey.put(tableId, "col1");
        String index = getShardingString(shardingKey, "");
        assertThat(index).isEqualTo("testc-10");
    }

    @Test
    void testTableShardingWithInteger() {
        HashMap<TableId, String> shardingKey = new HashMap<>();
        shardingKey.put(tableId, "id");
        String index = getShardingString(shardingKey, "");
        assertThat(index).isEqualTo("test110");
    }

    @Test
    void testTableShardingWithDate() {
        HashMap<TableId, String> shardingKey = new HashMap<>();
        shardingKey.put(tableId, "dt");
        String index = getShardingString(shardingKey, "");
        assertThat(index).isEqualTo("test2025-01-01");
    }

    @Test
    void testTableShardingWithNull() {
        HashMap<TableId, String> shardingKey = new HashMap<>();
        shardingKey.put(tableId, "col2");
        String index = getShardingString(shardingKey, "");
        assertThat(index).isEqualTo("test");
    }

    @Test
    void testTableShardingWithPartitionCol() {
        String index = getShardingString(Collections.emptyMap(), "_");
        assertThat(index).isEqualTo("test_2025-01-01");
    }

    @Test
    void testTableShardingWithSeparator() {
        String index = getShardingString(Collections.emptyMap(), "$");
        assertThat(index).isEqualTo("test$2025-01-01");
    }

    private String getShardingString(Map<TableId, String> shardingKey, String shardingSeparator) {
        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.INT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.FLOAT(),
                            DataTypes.VARCHAR(45),
                            DataTypes.VARCHAR(55),
                            DataTypes.TIMESTAMP_TZ(),
                            DataTypes.DATE()
                        },
                        new String[] {"id", "name", "weight", "col1", "col2", "create_time", "dt"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        DataChangeEvent event =
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    110,
                                    BinaryStringData.fromString("scooter"),
                                    5.5f,
                                    BinaryStringData.fromString("c-10"),
                                    null,
                                    ZonedTimestampData.fromZonedDateTime(
                                            LocalDateTime.of(2023, 11, 11, 11, 11, 11, 11)
                                                    .atZone(ZoneId.systemDefault())),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 1))
                                }));
        ElasticsearchEventSerializer serializer =
                new ElasticsearchEventSerializer(ZoneId.of("UTC"), shardingKey, shardingSeparator);
        Schema tableSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("weight", DataTypes.FLOAT())
                        .physicalColumn("col1", DataTypes.VARCHAR(45))
                        .physicalColumn("col2", DataTypes.VARCHAR(55))
                        .physicalColumn("create_time", DataTypes.TIMESTAMP_TZ())
                        .physicalColumn("dt", DataTypes.DATE())
                        .partitionKey("dt")
                        .build();

        serializer.apply(new CreateTableEvent(tableId, tableSchema), new MockContext());
        return serializer.apply(event, new MockContext())._toBulkOperation().index().index();
    }

    class MockContext implements SinkWriter.Context {

        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }
}
