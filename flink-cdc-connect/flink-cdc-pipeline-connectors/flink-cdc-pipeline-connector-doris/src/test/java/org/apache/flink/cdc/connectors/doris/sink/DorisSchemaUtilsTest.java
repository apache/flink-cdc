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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_EXCLUDE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_KEY;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.TABLE_CREATE_PARTITION_UNIT;

/** A test for {@link org.apache.flink.cdc.connectors.doris.utils.DorisSchemaUtils} . */
public class DorisSchemaUtilsTest {
    private static final String SPECIFIC_TABLE_IDENTIFIER = "doris_database.partition_table";
    private static final TableId TABLE_ID = TableId.parse(SPECIFIC_TABLE_IDENTIFIER);

    private static final Schema SCHEMA =
            Schema.newBuilder()
                    .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                    .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null, "2.71828"))
                    .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null, "Alice"))
                    .column(new PhysicalColumn("create_time", DataTypes.TIMESTAMP(), null, null))
                    .column(new PhysicalColumn("create_time2", DataTypes.TIMESTAMP(), null, null))
                    .primaryKey("id")
                    .build();

    @Test
    public void testPartitionInfoByIncludeAllAndDefaultPartitionKey() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isEqualTo(new Tuple2<>("create_time", "year"));
    }

    @Test
    public void testPartitionInfoByExclude() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_EXCLUDE, SPECIFIC_TABLE_IDENTIFIER);
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isNull();
    }

    @Test
    public void testPartitionInfoByIncludeSpecificTableAndDefaultPartitionKey() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, SPECIFIC_TABLE_IDENTIFIER);
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isEqualTo(new Tuple2<>("create_time", "year"));
    }

    @Test
    public void testPartitionInfoBySpecificTable() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create1_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "month");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_KEY,
                "create_time");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_UNIT,
                "year");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isEqualTo(new Tuple2<>("create_time", "year"));
    }

    @Test
    public void testPartitionInfoByNonDateOrDatetimeType() {
        Map<String, String> map = new HashMap<>();
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_KEY,
                "create_time");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_UNIT,
                "year");

        Configuration config = Configuration.fromMap(map);

        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null, "2.71828"))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null, "Alice"))
                        .column(new PhysicalColumn("create_time", DataTypes.STRING(), null, null))
                        .primaryKey("id")
                        .build();

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, schema, TABLE_ID);
        Assertions.assertThat(partitionInfo).isNull();
    }

    @Test
    public void testPartitionInfoByIncludeAllAndSpecificPartitionKey() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.\\.*");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_KEY,
                "create_time2");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_UNIT,
                "month");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isEqualTo(new Tuple2<>("create_time2", "month"));
    }

    @Test
    public void testPartitionInfoByIncludeAndExclude() {
        Map<String, String> map = new HashMap<>();
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_INCLUDE, "doris_database.p\\.*");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_EXCLUDE, "doris_database.partition_\\.*");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_KEY, "create_time");
        map.put(TABLE_CREATE_AUTO_PARTITION_PROPERTIES_DEFAULT_PARTITION_UNIT, "year");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_KEY,
                "create_time2");
        map.put(
                TABLE_CREATE_AUTO_PARTITION_PROPERTIES_PREFIX
                        + SPECIFIC_TABLE_IDENTIFIER
                        + "."
                        + TABLE_CREATE_PARTITION_UNIT,
                "month");

        Configuration config = Configuration.fromMap(map);

        Tuple2<String, String> partitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, TABLE_ID);
        Assertions.assertThat(partitionInfo).isNull();

        TableId newTableId = TableId.parse("doris_database.part_table");
        Tuple2<String, String> newPartitionInfo =
                DorisSchemaUtils.getPartitionInfo(config, SCHEMA, newTableId);
        Assertions.assertThat(newPartitionInfo).isEqualTo(new Tuple2<>("create_time", "year"));
    }
}
