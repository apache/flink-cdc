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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link OracleMetadataAccessor}. */
public class OracleMetadataAccessorITCase extends OracleSourceTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting Oracle containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Container Oracle is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping Oracle containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Container Oracle is stopped.");
    }

    @BeforeEach
    public void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testOracleAccessDatabaseAndTable() throws Exception {
        testAccessDatabaseAndTable();
    }

    @Test
    public void testOracleAccessCommonTypesSchema() throws Exception {
        testAccessCommonTypesSchema();
    }

    private void testAccessDatabaseAndTable() throws Exception {
        createAndInitialize("column_type_test.sql");
        String[] tables = new String[] {"full_types"};
        OracleMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        assertThatThrownBy(metadataAccessor::listNamespaces)
                .isInstanceOf(UnsupportedOperationException.class);

        List<String> schemas = metadataAccessor.listSchemas(null);
        assertThat(schemas).contains("DEBEZIUM");

        List<TableId> actualTables = metadataAccessor.listTables(null, "DEBEZIUM");
        List<TableId> expectedTables =
                Arrays.stream(tables)
                        .map(table -> TableId.tableId("debezium", table))
                        .collect(Collectors.toList());
        assertThat(actualTables).containsExactlyInAnyOrderElementsOf(expectedTables);
    }

    private void testAccessCommonTypesSchema() throws Exception {
        createAndInitialize("column_type_test.sql");
        String[] tables = new String[] {"FULL_TYPES"};
        OracleMetadataAccessor metadataAccessor = getMetadataAccessor(tables);

        Schema actualSchema =
                metadataAccessor.getTableSchema(
                        TableId.tableId(ORACLE_CONTAINER.getDatabaseName(), "FULL_TYPES"));
        Schema expectedSchema =
                Schema.newBuilder()
                        .primaryKey("ID")
                        .fromRowDataType(
                                RowType.of(
                                        new DataType[] {
                                            DataTypes.BIGINT().notNull(),
                                            DataTypes.VARCHAR(1000),
                                            DataTypes.VARCHAR(1000),
                                            DataTypes.VARCHAR(1000),
                                            DataTypes.CHAR(3),
                                            DataTypes.CHAR(3),
                                            DataTypes.FLOAT(),
                                            DataTypes.DOUBLE(),
                                            DataTypes.FLOAT(),
                                            DataTypes.FLOAT(),
                                            DataTypes.DECIMAL(10, 6),
                                            DataTypes.FLOAT(),
                                            DataTypes.FLOAT(),
                                            DataTypes.DECIMAL(10, 6),
                                            DataTypes.DECIMAL(10, 6),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP(2),
                                            DataTypes.TIMESTAMP(4),
                                            DataTypes.TIMESTAMP(6),
                                            DataTypes.TIMESTAMP_TZ(6),
                                            DataTypes.TIMESTAMP_LTZ(6),
                                            DataTypes.BIGINT(),
                                            DataTypes.BIGINT(),
                                            DataTypes.STRING()
                                        },
                                        new String[] {
                                            "ID",
                                            "VAL_VARCHAR",
                                            "VAL_VARCHAR2",
                                            "VAL_NVARCHAR2",
                                            "VAL_CHAR",
                                            "VAL_NCHAR",
                                            "VAL_BF",
                                            "VAL_BD",
                                            "VAL_F",
                                            "VAL_F_10",
                                            "VAL_NUM",
                                            "VAL_DP",
                                            "VAL_R",
                                            "VAL_DECIMAL",
                                            "VAL_NUMERIC",
                                            "VAL_NUM_VS",
                                            "VAL_INT",
                                            "VAL_INTEGER",
                                            "VAL_SMALLINT",
                                            "VAL_NUMBER_38_NO_SCALE",
                                            "VAL_NUMBER_38_SCALE_0",
                                            "VAL_NUMBER_1",
                                            "VAL_NUMBER_2",
                                            "VAL_NUMBER_4",
                                            "VAL_NUMBER_9",
                                            "VAL_NUMBER_18",
                                            "VAL_NUMBER_2_NEGATIVE_SCALE",
                                            "VAL_NUMBER_4_NEGATIVE_SCALE",
                                            "VAL_NUMBER_9_NEGATIVE_SCALE",
                                            "VAL_NUMBER_18_NEGATIVE_SCALE",
                                            "VAL_NUMBER_36_NEGATIVE_SCALE",
                                            "VAL_DATE",
                                            "VAL_TS",
                                            "VAL_TS_PRECISION2",
                                            "VAL_TS_PRECISION4",
                                            "VAL_TS_PRECISION9",
                                            "VAL_TSTZ",
                                            "VAL_TSLTZ",
                                            "VAL_INT_YTM",
                                            "VAL_INT_DTS",
                                            "T15VARCHAR"
                                        }))
                        .build();
        assertThat(actualSchema).isEqualTo(expectedSchema);
    }

    private OracleMetadataAccessor getMetadataAccessor(String[] tables) {
        OracleSourceConfig sourceConfig = getConfig(tables);
        return new OracleMetadataAccessor(sourceConfig);
    }

    private OracleSourceConfig getConfig(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> "DEBEZIUM." + tableName)
                        .toArray(String[]::new);
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(CONNECTOR_USER);
        configFactory.password(CONNECTOR_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList(captureTableIds);

        return configFactory.create(0);
    }
}
