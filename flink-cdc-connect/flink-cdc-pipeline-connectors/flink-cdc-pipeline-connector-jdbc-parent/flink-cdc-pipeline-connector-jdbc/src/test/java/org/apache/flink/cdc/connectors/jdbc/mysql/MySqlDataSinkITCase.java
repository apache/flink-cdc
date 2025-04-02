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

package org.apache.flink.cdc.connectors.jdbc.mysql;

import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/** IT cases for MySQL Data Sink. */
class MySqlDataSinkITCase extends MySqlSinkTestBase {

    private static final TableId TABLE_ID =
            TableId.tableId(
                    MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName(), "data_sink_test_table");

    @BeforeEach
    void initializeDatabase() {
        MySqlSinkTestBase.executeSql(
                String.format(
                        "CREATE DATABASE IF NOT EXISTS `%s`;",
                        MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName()));
        LOG.info("Database {} created.", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName());
    }

    @AfterEach
    void destroyDatabase() {
        MySqlSinkTestBase.executeSql(
                String.format(
                        "DROP DATABASE %s;", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName()));
        LOG.info("Database {} destroyed.", MySqlSinkTestBase.MYSQL_CONTAINER.getDatabaseName());
    }

    @Test
    void testSyncInsertEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            23, 1.41421356, BinaryStringData.fromString("Cicada")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            29, 9.973627192, BinaryStringData.fromString("Derrida")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly(
                        "17 | 3.141592653 | Alice",
                        "19 | 2.718281828 | Bob",
                        "23 | 1.41421356 | Cicada",
                        "29 | 9.973627192 | Derrida");
    }

    @Test
    void testSyncUpdateEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        }),
                                generator.generate(
                                        new Object[] {
                                            17, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 2.718281828 | Bob");
    }

    @Test
    void testSyncDeleteEvents() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.deleteEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO | PRI | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 3.141592653 | Alice");
    }

    @Test
    void testSyncInsertEventsWithoutPk() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            23, 1.41421356, BinaryStringData.fromString("Cicada")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            29, 9.973627192, BinaryStringData.fromString("Derrida")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO |  | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly(
                        "17 | 3.141592653 | Alice",
                        "19 | 2.718281828 | Bob",
                        "23 | 1.41421356 | Cicada",
                        "29 | 9.973627192 | Derrida");
    }

    @Test
    void testSyncUpdateEventsWithoutPk() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.updateEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        }),
                                generator.generate(
                                        new Object[] {
                                            17, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO |  | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 2.718281828 | Bob");
    }

    @Test
    void testSyncDeleteEventsWithoutPk() throws Exception {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .build();

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0]));

        runJobThatSinksToMySqlWithEvents(
                Arrays.asList(
                        new CreateTableEvent(TABLE_ID, schema),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            17, 3.141592653, BinaryStringData.fromString("Alice")
                                        })),
                        DataChangeEvent.insertEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        })),
                        DataChangeEvent.deleteEvent(
                                TABLE_ID,
                                generator.generate(
                                        new Object[] {
                                            19, 2.718281828, BinaryStringData.fromString("Bob")
                                        }))));

        Assertions.assertThat(inspectTableSchema(TABLE_ID))
                .containsExactly(
                        "id | int | NO |  | null",
                        "number | double | YES |  | null",
                        "name | varchar(17) | YES |  | null");

        Assertions.assertThat(inspectTableContent(TABLE_ID, 3))
                .containsExactly("17 | 3.141592653 | Alice");
    }
}
