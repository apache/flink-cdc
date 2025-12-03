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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE;

/** A test for {@link org.apache.flink.cdc.connectors.doris.sink.DorisEventSerializer} . */
public class DorisEventSerializerTest {

    private ObjectMapper objectMapper = new ObjectMapper();
    private static DorisEventSerializer dorisEventSerializer;

    private static final TableId TABLE_ID = TableId.parse("doris_database.doris_table");
    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .physicalColumn("id", DataTypes.STRING())
                    .physicalColumn("name", DataTypes.STRING())
                    .physicalColumn("create_date", DataTypes.DATE())
                    .physicalColumn("create_time", DataTypes.TIMESTAMP())
                    .primaryKey("id")
                    .build();
    private static final BinaryRecordDataGenerator RECORD_DATA_GENERATOR =
            new BinaryRecordDataGenerator(((RowType) SCHEMA.toRowDataType()));

    @Test
    public void testDataChangeEventWithDateTimePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(Instant.parse("2025-01-16T08:00:00Z"), ZoneId.of("Z"));
        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    TimestampData.fromLocalDateTime(localDateTime),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo("2025-01-16 08:00:00.000000");
    }

    @Test
    public void testDataChangeEventIfDatetimePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_time").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATETIME);
    }

    @Test
    public void testDataChangeEventWithDatePartitionColumn() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    DateData.fromLocalDate(LocalDate.of(2025, 1, 16)),
                                    null,
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText()).isEqualTo("2025-01-16");
    }

    @Test
    public void testDataChangeEventIfDatePartitionColumnIsNull() throws IOException {
        Map<String, String> configMap = new HashMap<>();
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_date");
        configMap.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration dorisConfig = Configuration.fromMap(configMap);
        dorisEventSerializer = new DorisEventSerializer(ZoneId.of("UTC"), dorisConfig);

        CreateTableEvent createTableEvent = new CreateTableEvent(TABLE_ID, SCHEMA);
        dorisEventSerializer.serialize(createTableEvent);

        DataChangeEvent dataChangeEvent =
                DataChangeEvent.insertEvent(
                        TABLE_ID,
                        RECORD_DATA_GENERATOR.generate(
                                new Object[] {
                                    new BinaryStringData("1"),
                                    new BinaryStringData("flink"),
                                    null,
                                    TimestampData.fromMillis(System.currentTimeMillis()),
                                }));

        DorisRecord dorisRecord = dorisEventSerializer.serialize(dataChangeEvent);
        JsonNode jsonNode = objectMapper.readTree(dorisRecord.getRow());
        Assertions.assertThat(jsonNode.get("create_date").asText())
                .isEqualTo(DorisSchemaUtils.DEFAULT_DATE);
    }
}
