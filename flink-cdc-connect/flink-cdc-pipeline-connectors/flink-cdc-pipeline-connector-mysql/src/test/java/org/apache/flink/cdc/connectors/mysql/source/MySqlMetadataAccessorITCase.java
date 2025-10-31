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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqSourceTestUtils;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link MySqlMetadataAccessor}. */
class MySqlMetadataAccessorITCase extends MySqlSourceTestBase {

    private final UniqueDatabase fullTypesMySqlDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER,
                    "column_type_test",
                    MySqSourceTestUtils.TEST_USER,
                    MySqSourceTestUtils.TEST_PASSWORD);

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeEach
    void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    void testMysqlAccessDatabaseAndTable() {
        testAccessDatabaseAndTable(fullTypesMySqlDatabase);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMysqlAccessCommonTypesSchema(boolean tinyInt1IsBit) {
        testAccessCommonTypesSchema(fullTypesMySqlDatabase, tinyInt1IsBit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testMysqlAccessTimeTypesSchema(boolean tinyInt1IsBit) {
        fullTypesMySqlDatabase.createAndInitialize();

        String[] tables = new String[] {"time_types"};
        MySqlMetadataAccessor metadataAccessor =
                getMetadataAccessor(tables, fullTypesMySqlDatabase, tinyInt1IsBit);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(fullTypesMySqlDatabase.getDatabaseName(), "time_types"));
        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.DECIMAL(20, 0).notNull(),
                                            DataTypes.INT(),
                                            DataTypes.DATE(),
                                            DataTypes.TIME(0),
                                            DataTypes.TIME(3),
                                            DataTypes.TIME(6),
                                            DataTypes.TIMESTAMP(0),
                                            DataTypes.TIMESTAMP(3),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP_LTZ(0),
                                            DataTypes.TIMESTAMP_LTZ(0)
                                        },
                                        new String[] {
                                            "id",
                                            "year_c",
                                            "date_c",
                                            "time_c",
                                            "time_3_c",
                                            "time_6_c",
                                            "datetime_c",
                                            "datetime3_c",
                                            "datetime6_c",
                                            "timestamp_c",
                                            "timestamp_def_c"
                                        }))
                        .build();
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMysqlPrecisionTypesSchema(boolean tinyInt1IsBit) {
        fullTypesMySqlDatabase.createAndInitialize();

        String[] tables = new String[] {"precision_types"};
        MySqlMetadataAccessor metadataAccessor =
                getMetadataAccessor(tables, fullTypesMySqlDatabase, tinyInt1IsBit);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(
                                fullTypesMySqlDatabase.getDatabaseName(), "precision_types"));
        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.DECIMAL(20, 0).notNull(),
                                            DataTypes.DECIMAL(6, 2),
                                            DataTypes.DECIMAL(9, 4),
                                            DataTypes.DECIMAL(20, 4),
                                            DataTypes.TIME(0),
                                            DataTypes.TIME(3),
                                            DataTypes.TIME(6),
                                            DataTypes.TIMESTAMP(0),
                                            DataTypes.TIMESTAMP(3),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP_LTZ(0),
                                            DataTypes.TIMESTAMP_LTZ(3),
                                            DataTypes.TIMESTAMP_LTZ(6),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE()
                                        },
                                        new String[] {
                                            "id",
                                            "decimal_c0",
                                            "decimal_c1",
                                            "decimal_c2",
                                            "time_c",
                                            "time_3_c",
                                            "time_6_c",
                                            "datetime_c",
                                            "datetime3_c",
                                            "datetime6_c",
                                            "timestamp_c",
                                            "timestamp3_c",
                                            "timestamp6_c",
                                            "float_c0",
                                            "float_c1",
                                            "float_c2",
                                            "real_c0",
                                            "real_c1",
                                            "real_c2",
                                            "double_c0",
                                            "double_c1",
                                            "double_c2",
                                            "double_precision_c0",
                                            "double_precision_c1",
                                            "double_precision_c2"
                                        }))
                        .build();
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private void testAccessDatabaseAndTable(UniqueDatabase database) {
        database.createAndInitialize();

        String[] tables =
                new String[] {"common_types", "time_types", "precision_types", "json_types"};
        MySqlMetadataAccessor metadataAccessor = getMetadataAccessor(tables, database, true);

        assertThatThrownBy(metadataAccessor::listNamespaces)
                .isInstanceOf(UnsupportedOperationException.class);

        List<String> schemas = metadataAccessor.listSchemas(null);
        assertThat(schemas).contains(database.getDatabaseName());

        List<TableId> actualTables = metadataAccessor.listTables(null, database.getDatabaseName());
        List<TableId> expectedTables =
                Arrays.stream(tables)
                        .map(table -> TableId.tableId(database.getDatabaseName(), table))
                        .collect(Collectors.toList());
        assertThat(actualTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }

    private void testAccessCommonTypesSchema(UniqueDatabase database, boolean tinyint1IsBit) {
        database.createAndInitialize();

        String[] tables = new String[] {"common_types"};
        MySqlMetadataAccessor metadataAccessor =
                getMetadataAccessor(tables, database, tinyint1IsBit);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(database.getDatabaseName(), "common_types"));
        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.DECIMAL(20, 0).notNull(),
                                            DataTypes.TINYINT(),
                                            DataTypes.SMALLINT(),
                                            DataTypes.SMALLINT(),
                                            DataTypes.SMALLINT(),
                                            DataTypes.INT(),
                                            DataTypes.INT(),
                                            DataTypes.INT(),
                                            DataTypes.INT(),
                                            DataTypes.INT(),
                                            DataTypes.INT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.INT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.DECIMAL(20, 0),
                                            DataTypes.DECIMAL(20, 0),
                                            DataTypes.VARCHAR(255),
                                            DataTypes.CHAR(3),
                                            DataTypes.DOUBLE(),
                                            DataTypes.FLOAT(),
                                            DataTypes.FLOAT(),
                                            DataTypes.FLOAT(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.DECIMAL(8, 4),
                                            DataTypes.DECIMAL(8, 4),
                                            DataTypes.DECIMAL(8, 4),
                                            DataTypes.DECIMAL(6, 0),
                                            // Decimal precision larger than 38 will be treated as
                                            // string.
                                            DataTypes.STRING(),
                                            DataTypes.BOOLEAN(),
                                            DataTypes.BINARY(1),
                                            tinyint1IsBit
                                                    ? DataTypes.BOOLEAN()
                                                    : DataTypes.TINYINT(),
                                            tinyint1IsBit
                                                    ? DataTypes.BOOLEAN()
                                                    : DataTypes.TINYINT(),
                                            DataTypes.BINARY(16),
                                            DataTypes.BINARY(8),
                                            DataTypes.STRING(),
                                            DataTypes.BYTES(),
                                            DataTypes.BYTES(),
                                            DataTypes.BYTES(),
                                            DataTypes.BYTES(),
                                            DataTypes.INT(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {
                                            "id",
                                            "tiny_c",
                                            "tiny_un_c",
                                            "tiny_un_z_c",
                                            "small_c",
                                            "small_un_c",
                                            "small_un_z_c",
                                            "medium_c",
                                            "medium_un_c",
                                            "medium_un_z_c",
                                            "int_c",
                                            "int_un_c",
                                            "int_un_z_c",
                                            "int11_c",
                                            "big_c",
                                            "big_un_c",
                                            "big_un_z_c",
                                            "varchar_c",
                                            "char_c",
                                            "real_c",
                                            "float_c",
                                            "float_un_c",
                                            "float_un_z_c",
                                            "double_c",
                                            "double_un_c",
                                            "double_un_z_c",
                                            "decimal_c",
                                            "decimal_un_c",
                                            "decimal_un_z_c",
                                            "numeric_c",
                                            "big_decimal_c",
                                            "bit1_c",
                                            "bit3_c",
                                            "tiny1_c",
                                            "boolean_c",
                                            "file_uuid",
                                            "bit_c",
                                            "text_c",
                                            "tiny_blob_c",
                                            "blob_c",
                                            "medium_blob_c",
                                            "long_blob_c",
                                            "year_c",
                                            "enum_c",
                                            "json_c",
                                            "point_c",
                                            "geometry_c",
                                            "linestring_c",
                                            "polygon_c",
                                            "multipoint_c",
                                            "multiline_c",
                                            "multipolygon_c",
                                            "geometrycollection_c",
                                            "long_c",
                                            "long_varchar_c",
                                            "varchar_len0_c"
                                        }))
                        .build();
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private MySqlMetadataAccessor getMetadataAccessor(
            String[] tables, UniqueDatabase database, boolean tinyint1IsBit) {
        MySqlSourceConfig sourceConfig = getConfig(tables, database, tinyint1IsBit);
        return new MySqlMetadataAccessor(sourceConfig);
    }

    @Test
    public void testMysqlAccessJsonTypesSchema() {
        fullTypesMySqlDatabase.createAndInitialize();

        String[] tables = new String[] {"json_types"};
        MySqlMetadataAccessor metadataAccessor =
                getMetadataAccessor(tables, fullTypesMySqlDatabase, true);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(fullTypesMySqlDatabase.getDatabaseName(), "json_types"));
        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.DECIMAL(20, 0).notNull(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.STRING(),
                                            DataTypes.INT()
                                        },
                                        new String[] {
                                            "id", "json_c0", "json_c1", "json_c2", "int_c",
                                        }))
                        .build();
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private MySqlSourceConfig getConfig(
            String[] captureTables, UniqueDatabase database, boolean tinyint1IsBit) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> database.getDatabaseName() + "." + tableName)
                        .toArray(String[]::new);

        return new MySqlSourceConfigFactory()
                .startupOptions(StartupOptions.latest())
                .databaseList(database.getDatabaseName())
                .tableList(captureTableIds)
                .includeSchemaChanges(false)
                .hostname(database.getHost())
                .port(database.getDatabasePort())
                .splitSize(10)
                .fetchSize(2)
                .username(database.getUsername())
                .password(database.getPassword())
                .serverTimeZone(ZoneId.of("UTC").toString())
                .treatTinyInt1AsBoolean(tinyint1IsBit)
                .createConfig(0);
    }
}
