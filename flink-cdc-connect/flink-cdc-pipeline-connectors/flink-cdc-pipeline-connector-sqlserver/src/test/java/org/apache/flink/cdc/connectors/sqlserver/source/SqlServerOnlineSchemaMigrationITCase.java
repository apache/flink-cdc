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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.sqlserver.testutils.SqlServerSourceTestUtils.fetchResults;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** IT case for evolving SQL Server schema during online migration. The flow mirrors */
class SqlServerOnlineSchemaMigrationITCase extends SqlServerTestBase {

    private static final String DATABASE_NAME = "customer";
    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @SuppressWarnings("deprecation")
    @BeforeEach
    void before() {
        initializeSqlServerTable(DATABASE_NAME);
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    void testSchemaMigrationFromScratch() throws Exception {
        env.setParallelism(1);

        TableId tableId = TableId.tableId(DATABASE_NAME, "dbo", "customers");
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.customers")
                                .startupOptions(StartupOptions.initial())
                                .includeSchemaChanges(true)
                                .serverTimeZone("UTC");

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new SqlServerDataSource(configFactory).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                SqlServerDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        List<Event> expected = new ArrayList<>();
        Schema schemaV1 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                        .physicalColumn("address", DataTypes.VARCHAR(1024))
                        .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                        .primaryKey(Collections.singletonList("id"))
                        .build();
        expected.add(new CreateTableEvent(tableId, schemaV1));
        expected.addAll(getSnapshotExpected(tableId, schemaV1));
        List<Event> actual = fetchResults(events, expected.size());
        Assertions.assertThat(actual.stream().map(Object::toString))
                .containsExactlyInAnyOrderElementsOf(
                        expected.stream().map(Object::toString).collect(Collectors.toList()));

        // ADD COLUMN
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute("ALTER TABLE dbo.customers ADD ext INT");
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers', "
                            + "@role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_customers_v2';");
            statement.execute(
                    "INSERT INTO dbo.customers VALUES (10000, 'Alice', 'Beijing', '123567891234', 17);");
        }

        Schema schemaV2 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                        .physicalColumn("address", DataTypes.VARCHAR(1024))
                        .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                        .physicalColumn("ext", DataTypes.INT())
                        .primaryKey(Collections.singletonList("id"))
                        .build();

        Assertions.assertThat(fetchResults(events, 2))
                .containsExactly(
                        new AddColumnEvent(
                                tableId,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                new PhysicalColumn("ext", DataTypes.INT(), null),
                                                AddColumnEvent.ColumnPosition.AFTER,
                                                "phone_number"))),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generate(schemaV2, 10000, "Alice", "Beijing", "123567891234", 17)));

        // MODIFY COLUMN
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute(
                    "EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'customers', "
                            + "@capture_instance = 'dbo_customers';");
            statement.execute("ALTER TABLE dbo.customers ALTER COLUMN ext FLOAT");
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers', "
                            + "@role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_customers_v3';");
            statement.execute(
                    "INSERT INTO dbo.customers VALUES (10001, 'Bob', 'Chongqing', '123567891234', 2.718281828);");
        }

        Schema schemaV3 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                        .physicalColumn("address", DataTypes.VARCHAR(1024))
                        .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                        .physicalColumn("ext", DataTypes.DOUBLE())
                        .primaryKey(Collections.singletonList("id"))
                        .build();

        Assertions.assertThat(fetchResults(events, 2))
                .containsExactly(
                        new AlterColumnTypeEvent(
                                tableId,
                                Collections.singletonMap("ext", DataTypes.DOUBLE()),
                                Map.of("ext", DataTypes.INT())),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generate(
                                        schemaV3,
                                        10001,
                                        "Bob",
                                        "Chongqing",
                                        "123567891234",
                                        2.718281828)));

        // DROP COLUMN
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute(
                    "EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'customers', "
                            + "@capture_instance = 'dbo_customers_v2';");
            statement.execute("ALTER TABLE dbo.customers DROP COLUMN ext");
            statement.execute(
                    "EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'customers', "
                            + "@role_name = NULL, @supports_net_changes = 0, @capture_instance = 'dbo_customers_v4';");
            statement.execute(
                    "INSERT INTO dbo.customers VALUES (10002, 'Cicada', 'Urumqi', '123567891234');");
        }

        Schema schemaV4 =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull(), null, "flink")
                        .physicalColumn("address", DataTypes.VARCHAR(1024))
                        .physicalColumn("phone_number", DataTypes.VARCHAR(512))
                        .primaryKey(Collections.singletonList("id"))
                        .build();

        Assertions.assertThat(fetchResults(events, 2))
                .containsExactly(
                        new DropColumnEvent(tableId, Collections.singletonList("ext")),
                        DataChangeEvent.insertEvent(
                                tableId,
                                generate(schemaV4, 10002, "Cicada", "Urumqi", "123567891234")));
    }

    private List<Event> getSnapshotExpected(TableId tableId, Schema schema) {
        return Stream.of(
                        generate(schema, 101, "user_1", "Shanghai", "123567891234"),
                        generate(schema, 102, "user_2", "Shanghai", "123567891234"),
                        generate(schema, 103, "user_3", "Shanghai", "123567891234"),
                        generate(schema, 109, "user_4", "Shanghai", "123567891234"),
                        generate(schema, 110, "user_5", "Shanghai", "123567891234"),
                        generate(schema, 111, "user_6", "Shanghai", "123567891234"),
                        generate(schema, 118, "user_7", "Shanghai", "123567891234"),
                        generate(schema, 121, "user_8", "Shanghai", "123567891234"),
                        generate(schema, 123, "user_9", "Shanghai", "123567891234"),
                        generate(schema, 1009, "user_10", "Shanghai", "123567891234"),
                        generate(schema, 1010, "user_11", "Shanghai", "123567891234"),
                        generate(schema, 1011, "user_12", "Shanghai", "123567891234"),
                        generate(schema, 1012, "user_13", "Shanghai", "123567891234"),
                        generate(schema, 1013, "user_14", "Shanghai", "123567891234"),
                        generate(schema, 1014, "user_15", "Shanghai", "123567891234"),
                        generate(schema, 1015, "user_16", "Shanghai", "123567891234"),
                        generate(schema, 1016, "user_17", "Shanghai", "123567891234"),
                        generate(schema, 1017, "user_18", "Shanghai", "123567891234"),
                        generate(schema, 1018, "user_19", "Shanghai", "123567891234"),
                        generate(schema, 1019, "user_20", "Shanghai", "123567891234"),
                        generate(schema, 2000, "user_21", "Shanghai", "123567891234"))
                .map(record -> DataChangeEvent.insertEvent(tableId, record))
                .collect(Collectors.toList());
    }

    private BinaryRecordData generate(Schema schema, Object... fields) {
        return (new BinaryRecordDataGenerator(schema.getColumnDataTypes().toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }
}
