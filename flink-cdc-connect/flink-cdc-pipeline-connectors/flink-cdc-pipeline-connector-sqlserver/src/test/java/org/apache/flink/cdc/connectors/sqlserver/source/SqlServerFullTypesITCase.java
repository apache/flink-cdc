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
import org.apache.flink.cdc.common.data.DateData;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimeData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.factory.SqlServerDataSourceFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.apache.flink.cdc.connectors.sqlserver.testutils.RecordDataTestUtils.recordFields;
import static org.apache.flink.cdc.connectors.sqlserver.testutils.SqlServerSourceTestUtils.fetchResultsAndCreateTableEvent;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** IT case for SQL Server full types support. */
public class SqlServerFullTypesITCase extends SqlServerTestBase {

    private static final String DATABASE_NAME = "column_type_test";

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @SuppressWarnings("deprecation")
    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
        initializeSqlServerTable(DATABASE_NAME);
    }

    @Test
    public void testFullTypes() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.full_types")
                                .startupOptions(StartupOptions.initial())
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

        // Verify basic types (id through bigint)
        Object[] expectedSnapshot =
                new Object[] {
                    // id
                    1,
                    // Character types
                    BinaryStringData.fromString("abc"),
                    BinaryStringData.fromString("varchar value"),
                    BinaryStringData.fromString("text value"),
                    BinaryStringData.fromString("中文 "),
                    BinaryStringData.fromString("nvarchar value"),
                    BinaryStringData.fromString("ntext value"),
                    // Numeric types
                    DecimalData.fromBigDecimal(new BigDecimal("123.456"), 6, 3),
                    DecimalData.fromBigDecimal(new BigDecimal("9876543.21"), 10, 2),
                    3.14159265358979d,
                    2.71828f,
                    DecimalData.fromBigDecimal(new BigDecimal("214748.3647"), 18, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("130567005.7988"), 18, 4),
                    // Boolean and integer types
                    true,
                    (short) 255,
                    (short) 32767,
                    2147483647,
                    9223372036854775807L,
                    // Date and time types
                    DateData.fromEpochDay(18460), // 2020-07-17
                    TimeData.fromMillisOfDay(64822120), // 18:00:22.12
                    TimeData.fromMillisOfDay(64822123), // 18:00:22.1234
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")), // datetime2
                    LocalZonedTimestampData.fromInstant(
                            toInstant("2020-07-17 18:00:22.1234567")), // datetime offset
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123")), // datetime
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:00")), // small datetime
                    // Other types
                    BinaryStringData.fromString("<root><child>value</child></root>"), // xml
                    new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, // binary(8)
                    new byte[] {72, 101, 108, 108, 111}, // varbinary "Hello"
                    BinaryStringData.fromString(
                            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
                                    .toUpperCase()) // uniqueidentifier
                };

        // Verify basic types match
        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, FULL_TYPES)).isEqualTo(expectedSnapshot);
    }

    @Test
    public void testTimeTypes() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.time_types")
                                .startupOptions(StartupOptions.initial())
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

        Object[] expectedSnapshot =
                new Object[] {
                    // id
                    1,
                    // Date type
                    DateData.fromEpochDay(18460), // 2020-07-17
                    // Time types with different precisions
                    TimeData.fromMillisOfDay(64822000), // 18:00:22 (precision 0)
                    TimeData.fromMillisOfDay(64822123), // 18:00:22.123 (precision 3)
                    TimeData.fromMillisOfDay(
                            64822123), // 18:00:22.123456 (precision 6, stored as millis)
                    // Datetime2 types with different precisions
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22")), // datetime2(0)
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123")), // datetime2(3)
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")), // datetime2(6)
                    // Datetimeoffset types with different precisions (UTC+00:00)
                    LocalZonedTimestampData.fromInstant(
                            toInstant("2020-07-17 18:00:22")), // datetimeoffset(0)
                    LocalZonedTimestampData.fromInstant(
                            toInstant("2020-07-17 18:00:22.123")), // datetimeoffset(3)
                    LocalZonedTimestampData.fromInstant(
                            toInstant("2020-07-17 18:00:22.123456")), // datetimeoffset(6)
                    // Datetime and smalldatetime
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123")), // datetime
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:00")) // smalldatetime
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Assertions.assertThat(recordFields(snapshotRecord, TIME_TYPES)).isEqualTo(expectedSnapshot);
    }

    @Test
    public void testPrecisionTypes() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.precision_types")
                                .startupOptions(StartupOptions.initial())
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

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("123456.7890"), 10, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("12345678901234.567890"), 20, 6),
                    DecimalData.fromBigDecimal(
                            new BigDecimal("1234567890123456789012345678.9012345678"), 38, 10),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.56"), 6, 2),
                    DecimalData.fromBigDecimal(new BigDecimal("123456.7890"), 10, 4),
                    3.141592653589793d,
                    2.7182818f,
                    DecimalData.fromBigDecimal(new BigDecimal("54975581.3896"), 19, 4),
                    DecimalData.fromBigDecimal(new BigDecimal("214748.3647"), 19, 4)
                };

        Object[] actualSnapshot = recordFields(snapshotRecord, PRECISION_TYPES);

        Assertions.assertThat(actualSnapshot).isEqualTo(expectedSnapshot);
    }

    @Test
    public void testStreamingUpdate() throws Exception {
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.precision_types")
                                .startupOptions(StartupOptions.initial())
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

        // Wait for snapshot to complete
        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        Assertions.assertThat(snapshotResults).hasSize(1);

        // Perform update
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute(
                    "UPDATE dbo.precision_types SET val_decimal_6_2 = 9999.99 WHERE id = 1");
        }

        // Verify update event
        List<Event> streamResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        Assertions.assertThat(streamResults).hasSize(1);

        RecordData afterRecord = ((DataChangeEvent) streamResults.get(0)).after();
        Object[] afterFields = recordFields(afterRecord, PRECISION_TYPES);

        // Verify updated value
        Assertions.assertThat(afterFields[1])
                .isEqualTo(DecimalData.fromBigDecimal(new BigDecimal("9999.99"), 6, 2));
    }

    private Instant toInstant(String ts) {
        return Timestamp.valueOf(ts).toLocalDateTime().atZone(ZoneId.of("UTC")).toInstant();
    }

    private static final RowType FULL_TYPES =
            RowType.of(
                    // id
                    DataTypes.INT().notNull(),
                    // Character types
                    DataTypes.CHAR(3),
                    DataTypes.VARCHAR(1000),
                    DataTypes.STRING(),
                    DataTypes.CHAR(3),
                    DataTypes.VARCHAR(1000),
                    DataTypes.STRING(),
                    // Numeric types
                    DataTypes.DECIMAL(6, 3),
                    DataTypes.DECIMAL(10, 2),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.DECIMAL(18, 4), // smallmoney - use precision 18 for compact storage
                    DataTypes.DECIMAL(18, 4), // money - use precision 18 for compact storage
                    // Boolean and integer types
                    DataTypes.BOOLEAN(),
                    DataTypes.SMALLINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    // Date and time types
                    DataTypes.DATE(),
                    DataTypes.TIME(2),
                    DataTypes.TIME(4),
                    DataTypes.TIMESTAMP(7),
                    DataTypes.TIMESTAMP_LTZ(7),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(0),
                    // Other types
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.BYTES(),
                    DataTypes.STRING());

    private static final RowType TIME_TYPES =
            RowType.of(
                    DataTypes.INT().notNull(),
                    DataTypes.DATE(),
                    DataTypes.TIME(0),
                    DataTypes.TIME(3),
                    DataTypes.TIME(6),
                    DataTypes.TIMESTAMP(0),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP_LTZ(0),
                    DataTypes.TIMESTAMP_LTZ(3),
                    DataTypes.TIMESTAMP_LTZ(6),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(0));

    private static final RowType PRECISION_TYPES =
            RowType.of(
                    DataTypes.INT().notNull(),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.DECIMAL(10, 4),
                    DataTypes.DECIMAL(20, 6),
                    DataTypes.DECIMAL(38, 10),
                    DataTypes.DECIMAL(6, 2),
                    DataTypes.DECIMAL(10, 4),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.DECIMAL(18, 4), // money - use precision 18 for compact storage
                    DataTypes.DECIMAL(18, 4)); // smallmoney - use precision 18 for compact storage
}
