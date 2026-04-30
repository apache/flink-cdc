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

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.sqlserver.SqlServerTestBase;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.utils.SqlServerSchemaUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.containers.MSSQLServerContainer.MS_SQL_SERVER_PORT;

/** IT cases for {@link SqlServerMetadataAccessor}. */
public class SqlServerMetadataAccessorITCase extends SqlServerTestBase {

    private static final String DATABASE_NAME = "column_type_test";

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(200);
        initializeSqlServerTable(DATABASE_NAME);
    }

    @Test
    public void testListNamespaces() {
        String[] tables = new String[] {"dbo.full_types"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        List<String> namespaces = metadataAccessor.listNamespaces();
        assertThat(namespaces).contains(DATABASE_NAME);
    }

    @Test
    public void testListSchemas() {
        String[] tables = new String[] {"dbo.full_types"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        List<String> schemas = metadataAccessor.listSchemas(DATABASE_NAME);
        assertThat(schemas).contains("dbo");
    }

    @Test
    public void testListSchemasUsesConfiguredDatabaseWhenNamespaceIsNull() {
        String[] tables = new String[] {"dbo.full_types"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        assertThat(metadataAccessor.listSchemas(null)).contains("dbo");
    }

    @Test
    public void testListTables() {
        String[] tables =
                new String[] {
                    "dbo.full_types", "dbo.time_types", "dbo.precision_types", "axo.precision"
                };
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        List<TableId> actualTables = metadataAccessor.listTables(DATABASE_NAME, "dbo");

        List<TableId> expectedTables =
                Arrays.asList(
                        TableId.tableId(DATABASE_NAME, "dbo", "full_types"),
                        TableId.tableId(DATABASE_NAME, "dbo", "time_types"),
                        TableId.tableId(DATABASE_NAME, "dbo", "precision_types"));

        assertThat(actualTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }

    @Test
    public void testListTablesFiltersBySchemaName() throws Exception {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("USE " + DATABASE_NAME);
            statement.execute("CREATE SCHEMA extra");
            statement.execute("CREATE TABLE extra.schema_only_table (id INT NOT NULL PRIMARY KEY)");
        }

        String[] tables = new String[] {"dbo.full_types", "extra.schema_only_table"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        assertThat(metadataAccessor.listTables(DATABASE_NAME, "dbo"))
                .containsExactly(TableId.tableId(DATABASE_NAME, "dbo", "full_types"));
        assertThat(metadataAccessor.listTables(DATABASE_NAME, "extra"))
                .containsExactly(TableId.tableId(DATABASE_NAME, "extra", "schema_only_table"));
    }

    @Test
    public void testGetSqlServerDialectCreatesPerConfigInstances() {
        SqlServerSourceConfigFactory fullTypesFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.full_types")
                                .serverTimeZone("UTC");
        SqlServerSourceConfigFactory precisionTypesFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList("dbo.precision_types")
                                .serverTimeZone("UTC");

        assertThat(SqlServerSchemaUtils.getSqlServerDialect(fullTypesFactory.create(0)))
                .isNotSameAs(
                        SqlServerSchemaUtils.getSqlServerDialect(precisionTypesFactory.create(0)));
    }

    @Test
    public void testAccessTimeTypesSchema() {
        String[] tables = new String[] {"dbo.time_types"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(DATABASE_NAME, "dbo", "time_types"));

        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.INT().notNull(),
                                            DataTypes.DATE(),
                                            DataTypes.TIME(0),
                                            DataTypes.TIME(3),
                                            DataTypes.TIME(6),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP(3),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP_LTZ(7),
                                            DataTypes.TIMESTAMP_LTZ(3),
                                            DataTypes.TIMESTAMP_LTZ(6),
                                            DataTypes.TIMESTAMP(3),
                                            DataTypes.TIMESTAMP(6)
                                        },
                                        new String[] {
                                            "id",
                                            "val_date",
                                            "val_time_0",
                                            "val_time_3",
                                            "val_time_6",
                                            "val_datetime2_0",
                                            "val_datetime2_3",
                                            "val_datetime2_6",
                                            "val_datetimeoffset_0",
                                            "val_datetimeoffset_3",
                                            "val_datetimeoffset_6",
                                            "val_datetime",
                                            "val_smalldatetime"
                                        }))
                        .build();

        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    @Test
    public void testAccessPrecisionTypesSchema() {
        String[] tables = new String[] {"dbo.precision_types"};
        SqlServerMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(DATABASE_NAME, "dbo", "precision_types"));

        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("id")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.INT().notNull(),
                                            DataTypes.DECIMAL(6, 2),
                                            DataTypes.DECIMAL(10, 4),
                                            DataTypes.DECIMAL(20, 6),
                                            DataTypes.DECIMAL(38, 10),
                                            DataTypes.DECIMAL(6, 2),
                                            DataTypes.DECIMAL(10, 4),
                                            DataTypes.DOUBLE(),
                                            DataTypes.FLOAT(),
                                            DataTypes.DECIMAL(19, 4),
                                            DataTypes.DECIMAL(10, 4)
                                        },
                                        new String[] {
                                            "id",
                                            "val_decimal_6_2",
                                            "val_decimal_10_4",
                                            "val_decimal_20_6",
                                            "val_decimal_38_10",
                                            "val_numeric_6_2",
                                            "val_numeric_10_4",
                                            "val_float",
                                            "val_real",
                                            "val_money",
                                            "val_smallmoney"
                                        }))
                        .build();

        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private SqlServerMetadataAccessor getMetadataAccessor(String[] tables) {
        // Debezium SQL Server table.include.list uses schema.table format (without database prefix)
        SqlServerSourceConfigFactory configFactory =
                (SqlServerSourceConfigFactory)
                        new SqlServerSourceConfigFactory()
                                .hostname(MSSQL_SERVER_CONTAINER.getHost())
                                .port(MSSQL_SERVER_CONTAINER.getMappedPort(MS_SQL_SERVER_PORT))
                                .username(MSSQL_SERVER_CONTAINER.getUsername())
                                .password(MSSQL_SERVER_CONTAINER.getPassword())
                                .databaseList(DATABASE_NAME)
                                .tableList(tables)
                                .serverTimeZone("UTC");

        SqlServerSourceConfig sourceConfig = configFactory.create(0);

        return new SqlServerMetadataAccessor(sourceConfig);
    }
}
