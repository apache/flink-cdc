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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.oracle.factory.OracleDataSourceFactory;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.util.CloseableIterator;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.lifecycle.Startables;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

/** IT tests for {@link OracleDataSource}. */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OraclePipelineITCase extends OracleSourceTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void before() throws Exception {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        Connection conn = getJdbcConnectionAsDBA();
        conn.createStatement().execute("GRANT ANALYZE ANY TO " + CONNECTOR_USER);
    }

    @Test
    @Order(1)
    public void testAlterAddAllColumnTypeStatement() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCAN_STARTUP_MODE.key(), "latest-offset");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.connection.adapter", "logminer");
        dbzProperties.setProperty("log.mining.strategy", "online_catalog");
        dbzProperties.setProperty("snapshot.locking.mode", "none");
        dbzProperties.setProperty("database.history.store.only.captured.tables.ddl", "true");
        dbzProperties.setProperty("include.schema.changes", "true");
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(CONNECTOR_USER);
        configFactory.password(CONNECTOR_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList("DEBEZIUM.PRODUCTS");
        configFactory.startupOptions(StartupOptions.initial());
        configFactory.includeSchemaChanges(true);
        configFactory.debeziumProperties(dbzProperties);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSource(
                                        configFactory,
                                        context.getFactoryConfiguration(),
                                        new ArrayList<>())
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(50_000);

        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId));
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(), dataChangeEvent.after());
                                })
                        .collect(Collectors.toList());
        expected.addAll(expectedSnapshot);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expected.addAll(executeAlterAndProvideExpected(tableId, statement));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS1 CHAR(10)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS2 VARCHAR2(100)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS1", DataTypes.CHAR(10))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS2", DataTypes.VARCHAR(100))))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS3 NCHAR(10)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS4 NVARCHAR2(100)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS3", DataTypes.CHAR(10))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS4", DataTypes.VARCHAR(100))))));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS5 NUMBER(10,2)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS5", DataTypes.DECIMAL(10, 2))))));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS6 INTEGER", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS6", DataTypes.INT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS7 SMALLINT", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS7", DataTypes.INT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS8 DECIMAL(5,2)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS8", DataTypes.DECIMAL(5, 2))))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS9 FLOAT   (10)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS9", DataTypes.FLOAT())))));
            statement.execute(String.format("ALTER TABLE %s.PRODUCTS ADD COLS10 REAL", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS10", DataTypes.FLOAT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS11 BINARY_FLOAT", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS11", DataTypes.FLOAT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS12 BINARY_DOUBLE", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS12", DataTypes.DOUBLE())))));
            statement.execute(String.format("ALTER TABLE %s.PRODUCTS ADD COLS13 DATE", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS13", DataTypes.DATE())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS14 TIMESTAMP", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS14", DataTypes.TIMESTAMP())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS15 TIMESTAMP WITH TIME ZONE",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS15", DataTypes.TIMESTAMP_TZ())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS16 INTERVAL YEAR TO MONTH",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS16", DataTypes.BIGINT())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS17 INTERVAL DAY TO SECOND",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS17", DataTypes.BIGINT())))));
            statement.execute(String.format("ALTER TABLE %s.PRODUCTS ADD COLS18 CLOB", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS18", DataTypes.STRING())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS19 NCLOB", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS19", DataTypes.STRING())))));
            statement.execute(String.format("ALTER TABLE %s.PRODUCTS ADD COLS20 BLOB", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS20", DataTypes.BYTES())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS21 RAW(100)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS21", DataTypes.BYTES())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS22 BFILE", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS22", DataTypes.BYTES())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS23 TIMESTAMP     WITH LOCAL TIME ZONE",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS23", DataTypes.TIMESTAMP_LTZ())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS25 ROWID", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS25", DataTypes.VARCHAR(18))))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS27 NUMBER", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS27", DataTypes.BIGINT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS28 TIMESTAMP(2)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS28", DataTypes.TIMESTAMP(2))))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS29 TIMESTAMP  (9) WITH   TIME ZONE",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS29", DataTypes.TIMESTAMP_TZ(9))))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS30 TIMESTAMP(6) WITH LOCAL TIME ZONE",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS30", DataTypes.TIMESTAMP_LTZ(6))))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS31 INTERVAL YEAR(2) TO MONTH",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS31", DataTypes.BIGINT())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS32 INTERVAL DAY(3) TO SECOND(2)",
                            "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS32", DataTypes.BIGINT())))));
            statement.execute(String.format("ALTER TABLE %s.PRODUCTS ADD COLS33 LONG", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS33", DataTypes.BIGINT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS34 SYS.XMLTYPE", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS34", DataTypes.STRING())))));
            statement.execute(
                    String.format(
                            "ALTER TABLE %s.PRODUCTS ADD COLS35 MDSYS.SDO_GEOMETRY", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS35", DataTypes.STRING())))));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS1", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS1")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS2", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS2")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS3", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS3")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS4", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS4")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS5", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS5")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS6", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS6")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS7", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS7")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS8", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS8")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS9", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS9")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS10", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS10")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS11", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS11")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS12", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS12")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS13", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS13")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS14", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS14")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS15", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS15")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS16", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS16")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS17", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS17")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS18", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS18")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS19", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS19")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS20", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS20")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS21", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS21")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS22", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS22")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS23", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS23")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS25", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS25")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS27", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS27")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS28", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS28")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS29", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS29")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS30", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS30")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS31", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS31")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS32", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS32")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS33", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS33")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS34", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS34")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS35", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS35")));
        }
        List<Event> actual = fetchResults(events, expected.size());
        List<String> actualStr = actual.stream().map(Object::toString).collect(Collectors.toList());
        assertThat(actualStr)
                .containsExactlyInAnyOrder(
                        expected.stream().map(Object::toString).toArray(String[]::new));
    }

    @Test
    @Order(2)
    public void testParseAlterStatement() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCAN_STARTUP_MODE.key(), "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.connection.adapter", "logminer");
        dbzProperties.setProperty("log.mining.strategy", "online_catalog");
        dbzProperties.setProperty("snapshot.locking.mode", "none");
        dbzProperties.setProperty("database.history.store.only.captured.tables.ddl", "true");
        dbzProperties.setProperty("include.schema.changes", "true");
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(CONNECTOR_USER);
        configFactory.password(CONNECTOR_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList("DEBEZIUM.PRODUCTS");
        configFactory.startupOptions(StartupOptions.initial());
        configFactory.includeSchemaChanges(true);
        configFactory.debeziumProperties(dbzProperties);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSource(
                                        configFactory,
                                        context.getFactoryConfiguration(),
                                        new ArrayList<>())
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(150_000);

        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        List<Event> expected = new ArrayList<>();
        expected.add(getProductsCreateTableEvent(tableId));
        List<Event> expectedSnapshot =
                getSnapshotExpected(tableId).stream()
                        .map(
                                event -> {
                                    DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
                                    return DataChangeEvent.insertEvent(
                                            dataChangeEvent.tableId(), dataChangeEvent.after());
                                })
                        .collect(Collectors.toList());
        expected.addAll(expectedSnapshot);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expected.addAll(executeAlterAndProvideExpected(tableId, statement));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS1 VARCHAR(45)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS2 VARCHAR(55)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS1", DataTypes.VARCHAR(45))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS2", DataTypes.VARCHAR(55))))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS3 VARCHAR(45)", "DEBEZIUM"));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS4 VARCHAR(55)", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS3", DataTypes.VARCHAR(45))))));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn(
                                                    "COLS4", DataTypes.VARCHAR(55))))));

            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS ADD COLS5 LONG NULL", "DEBEZIUM"));
            expected.add(
                    new AddColumnEvent(
                            tableId,
                            Collections.singletonList(
                                    new AddColumnEvent.ColumnWithPosition(
                                            Column.physicalColumn("COLS5", DataTypes.BIGINT())))));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS1", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS1")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS2", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS2")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS3", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS3")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS4", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS4")));
            statement.execute(
                    String.format("ALTER TABLE %s.PRODUCTS DROP COLUMN COLS5", "DEBEZIUM"));
            expected.add(new DropColumnEvent(tableId, Collections.singletonList("COLS5")));
        }
        List<Event> actual = fetchResults(events, expected.size());
        List<String> actualStr = actual.stream().map(Object::toString).collect(Collectors.toList());
        assertThat(actualStr)
                .containsExactlyInAnyOrder(
                        expected.stream().map(Object::toString).toArray(String[]::new));
    }

    @Test
    @Order(3)
    public void testInitialStartupMode() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(SCAN_STARTUP_MODE.key(), "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("database.connection.adapter", "logminer");
        dbzProperties.setProperty("log.mining.strategy", "online_catalog");
        dbzProperties.setProperty("snapshot.locking.mode", "none");
        dbzProperties.setProperty("database.history.store.only.captured.tables.ddl", "true");
        dbzProperties.setProperty("include.schema.changes", "true");
        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory
                .username(CONNECTOR_USER)
                .password(CONNECTOR_PWD)
                .port(ORACLE_CONTAINER.getOraclePort())
                .databaseList(ORACLE_CONTAINER.getDatabaseName())
                .hostname(ORACLE_CONTAINER.getHost())
                .tableList("DEBEZIUM.PRODUCTS")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(dbzProperties)
                .includeSchemaChanges(true);
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSource(
                                        configFactory,
                                        context.getFactoryConfiguration(),
                                        new ArrayList<>())
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(150_000);

        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        CreateTableEvent createTableEvent = getProductsCreateTableEvent(tableId);

        // generate snapshot data
        List<Event> expectedSnapshot = new ArrayList<>();
        expectedSnapshot.add(createTableEvent);
        expectedSnapshot.addAll(getSnapshotExpected(tableId));

        List<Event> expectedBinlog = new ArrayList<>();
        List<Event> actual;
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));

            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.FLOAT()
                            },
                            new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO %s.products VALUES (110,'jack','13V jack','5.5')",
                            "debezium")); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("13V jack"),
                                        5.5f
                                    })));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.products VALUES (111,'football','31o football',6.6)",
                            "debezium")); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("31o football"),
                                        6.6f
                                    })));
            statement.execute(
                    String.format(
                            "UPDATE %s.products SET NAME='peter', WEIGHT='6.7' WHERE id=110",
                            "debezium"));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("13V jack"),
                                        5.5f
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("peter"),
                                        BinaryStringData.fromString("13V jack"),
                                        6.7f
                                    })));
            statement.execute(String.format("DELETE FROM %s.products WHERE id = 111", "debezium"));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("31o football"),
                                        6.6f
                                    })));
            statement.execute(String.format("TRUNCATE TABLE %s.products", "DEBEZIUM"));

            // In this configuration, several subtasks might emit their corresponding
            // CreateTableEvent
            // to downstream. Since it is not possible to predict how many CreateTableEvents should
            // we
            // expect, we simply filter them out from expected sets, and assert there's at least
            // one.
            actual = fetchResultsExcept(events, expectedSnapshot.size() + expectedBinlog.size());
        }
        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps = new HashMap<>();
        String tableIdStr = ((CreateTableEvent) actual.get(0)).tableId().toString();
        TableId tableIdTmp =
                TableId.tableId(
                        tableIdStr.split("\\.")[0].toUpperCase(),
                        tableIdStr.split("\\.")[1].toUpperCase());
        Schema schema = ((CreateTableEvent) actual.get(0)).getSchema();
        fieldGetterMaps.put(tableIdTmp, SchemaUtils.createFieldGetters(schema));
        StringBuilder actualSnapshotStr =
                getResultString(actual.subList(0, expectedSnapshot.size()), fieldGetterMaps, false);
        StringBuilder expectedSnapshotStr =
                getResultString(expectedSnapshot, fieldGetterMaps, false);
        assertThat(actualSnapshotStr.toString()).isEqualTo(expectedSnapshotStr.toString());
        StringBuilder actualBinlogStr =
                getResultString(
                        actual.subList(expectedSnapshot.size(), actual.size()),
                        fieldGetterMaps,
                        false);
        StringBuilder expectedBinlogStr = getResultString(expectedBinlog, fieldGetterMaps, false);
        assertThat(actualBinlogStr.toString()).isEqualTo(expectedBinlogStr.toString());
    }

    @NotNull
    private StringBuilder getResultString(
            List<Event> events,
            Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps,
            boolean metaFlag) {
        StringBuilder sb = new StringBuilder();
        List<String> list = new ArrayList<>();
        for (Event event : events) {
            if (event instanceof DataChangeEvent) {
                list.add(
                        convertEventToStr(
                                event,
                                fieldGetterMaps.get(((ChangeEvent) event).tableId()),
                                metaFlag));
            } else {
                list.add(event.toString());
            }
        }
        Collections.sort(list);
        for (String s : list) {
            sb.append(s);
            sb.append("\r\n");
        }
        return sb;
    }

    private static <T> List<T> fetchResultsExcept(Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            result.add(event);
            size--;
        }
        return result;
    }

    public static String convertEventToStr(
            Event event, List<RecordData.FieldGetter> fieldGetters, boolean metaFlag) {
        if (event instanceof SchemaChangeEvent) {
            return event.toString();
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            return "DataChangeEvent{"
                    + "tableId="
                    + dataChangeEvent.tableId()
                    + ", before="
                    + getFields(fieldGetters, dataChangeEvent.before())
                    + ", after="
                    + getFields(fieldGetters, dataChangeEvent.after())
                    + ", op="
                    + dataChangeEvent.op()
                    + ", meta="
                    + (metaFlag ? describeMeta() : dataChangeEvent.describeMeta())
                    + '}';
        }
        return "Event{}";
    }

    public static String describeMeta() {
        StringBuilder stringBuilder = new StringBuilder("(");
        Map<String, String> metaMap = new HashMap<>();
        metaMap.put("op_ts", "0");
        stringBuilder.append(metaMap);
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    private static List<Object> getFields(
            List<RecordData.FieldGetter> fieldGetters, RecordData recordData) {
        List<Object> fields = new ArrayList<>(fieldGetters.size());
        if (recordData == null) {
            return fields;
        }
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            fields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return fields;
    }

    @Test
    @Order(5)
    public void testGeometryType() throws Exception {
        createAndInitialize("geometry.sql");
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.mylake");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(METADATA_LIST.key(), "op_ts");
        options.put(SCAN_STARTUP_MODE.key(), "initial");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);
        TableId tableId = TableId.tableId("DEBEZIUM", "MYLAKE");
        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        TableId.tableId("DEBEZIUM", "MYLAKE"),
                        Schema.newBuilder()
                                .physicalColumn("feature_id", DataTypes.BIGINT().notNull())
                                .physicalColumn("name", DataTypes.VARCHAR(32))
                                .physicalColumn("shape", DataTypes.STRING())
                                .primaryKey(Arrays.asList("feature_id"))
                                .build());

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT(), DataTypes.VARCHAR(32), DataTypes.STRING()
                        },
                        new String[] {"FEATURE_ID", "NAME", "SHAPE"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
        List<Event> expectedBinlog = new ArrayList<>();
        expectedBinlog.add(createTableEvent);

        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    1L,
                                    BinaryStringData.fromString("center"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[116.6,24.343]]\",\"type\":\"Point\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    2L,
                                    BinaryStringData.fromString("two-dimensional point"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[10.0,5.0]]\",\"type\":\"Point\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    3L,
                                    BinaryStringData.fromString("straight line segment"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[10.0,10.0],[20.0,20.0],[30.0,30.0]]\",\"type\":\"LineString\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    4L,
                                    BinaryStringData.fromString("polyline"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[5.0,5.0],[15.0,10.0],[25.0,5.0],[35.0,10.0]]\",\"type\":\"LineString\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    5L,
                                    BinaryStringData.fromString("rectangle"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0]]\",\"type\":\"Polygon\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    6L,
                                    BinaryStringData.fromString("Multi-Point"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[10.0,10.0],[20.0,20.0],[30.0,30.0]]\",\"type\":\"MultiPoint\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    7L,
                                    BinaryStringData.fromString("Multi line collection"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[10.0,10.0],[20.0,10.0],[10.0,20.0],[20.0,20.0]]\",\"type\":\"MultiLineString\",\"srid\":4326}")
                                })));

        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    8L,
                                    BinaryStringData.fromString("Multi-Polygon"),
                                    BinaryStringData.fromString(
                                            "{\"coordinates\":\"[[0.0,0.0],[10.0,0.0],[10.0,10.0],[0.0,10.0],[0.0,0.0],[15.0,15.0],[25.0,15.0],[25.0,25.0],[15.0,25.0],[15.0,15.0]]\",\"type\":\"MultiPolygon\",\"srid\":4326}")
                                })));
        expectedBinlog.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    9L,
                                    BinaryStringData.fromString("Geometry Collection"),
                                    BinaryStringData.fromString(
                                            "{\"geometries\":\"GEOMETRYCOLLECTION (POINT (25 25), LINESTRING (30 30, 35 25), POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0)))\",\"type\":\"GeometryCollection\",\"srid\":4326}")
                                })));
        List<Event> actual = fetchResults(events, expectedBinlog.size());
        assertThat(actual.get(0).toString()).isEqualTo(createTableEvent.toString());

        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps = new HashMap<>();
        String tableIdStr = ((CreateTableEvent) actual.get(0)).tableId().toString();
        TableId tableId2 =
                TableId.tableId(
                        tableIdStr.split("\\.")[0].toUpperCase(),
                        tableIdStr.split("\\.")[1].toUpperCase());
        Schema schema = createTableEvent.getSchema();
        fieldGetterMaps.put(tableId2, SchemaUtils.createFieldGetters(schema));
        StringBuilder actualStr = getResultString(actual, fieldGetterMaps, true);
        StringBuilder expectedStr = getResultString(expectedBinlog, fieldGetterMaps, true);
        assertThat(actualStr.toString()).isEqualTo(expectedStr.toString());
    }

    @Test
    @Order(4)
    public void testInitialStartupModeWithOpTs() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(METADATA_LIST.key(), "op_ts");
        options.put(SCAN_STARTUP_MODE.key(), "latest-offset");
        Factory.Context context = new MockContext(Configuration.fromMap(options));
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new OracleDataSourceFactory()
                                .createDataSource(context)
                                .getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                OracleDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        Thread.sleep(5_000);
        TableId tableId = TableId.tableId("DEBEZIUM", "PRODUCTS");
        CreateTableEvent createTableEvent =
                getProductsCreateTableEvent(TableId.tableId("DEBEZIUM", "PRODUCTS"));
        // generate snapshot data
        Map<String, String> meta = new HashMap<>();
        meta.put("op_ts", "0");

        List<Event> expectedBinlog = new ArrayList<>();
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            expectedBinlog.addAll(executeAlterAndProvideExpected(tableId, statement));
            expectedBinlog.add(createTableEvent);
            RowType rowType =
                    RowType.of(
                            new DataType[] {
                                DataTypes.BIGINT().notNull(),
                                DataTypes.VARCHAR(255).notNull(),
                                DataTypes.VARCHAR(512),
                                DataTypes.FLOAT()
                            },
                            new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
            BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);
            // insert more data
            statement.execute(
                    String.format(
                            "INSERT INTO %s.PRODUCTS VALUES (110,'scooter','c-10',5.5)",
                            "DEBEZIUM")); // 110
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-10"),
                                        5.5f
                                    }),
                            meta));
            statement.execute(
                    String.format(
                            "INSERT INTO %s.PRODUCTS VALUES (111,'football','c-11',6.6)",
                            "DEBEZIUM")); // 111
            expectedBinlog.add(
                    DataChangeEvent.insertEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6f
                                    }),
                            meta));
            statement.execute(
                    String.format(
                            "UPDATE %s.PRODUCTS SET NAME='jack', WEIGHT=6.7 WHERE id=110",
                            "DEBEZIUM"));
            expectedBinlog.add(
                    DataChangeEvent.updateEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("scooter"),
                                        BinaryStringData.fromString("c-10"),
                                        5.5f
                                    }),
                            generator.generate(
                                    new Object[] {
                                        110L,
                                        BinaryStringData.fromString("jack"),
                                        BinaryStringData.fromString("c-10"),
                                        6.7f
                                    }),
                            meta));
            statement.execute(String.format("DELETE FROM %s.PRODUCTS WHERE id = 111", "DEBEZIUM"));
            expectedBinlog.add(
                    DataChangeEvent.deleteEvent(
                            tableId,
                            generator.generate(
                                    new Object[] {
                                        111L,
                                        BinaryStringData.fromString("football"),
                                        BinaryStringData.fromString("c-11"),
                                        6.6f
                                    }),
                            meta));
        }
        List<Event> actual = fetchResults(events, expectedBinlog.size());
        assertThat(actual.get(0).toString()).isEqualTo(createTableEvent.toString());

        Map<TableId, List<RecordData.FieldGetter>> fieldGetterMaps = new HashMap<>();
        String tableIdStr = ((CreateTableEvent) actual.get(0)).tableId().toString();
        TableId tableId2 =
                TableId.tableId(
                        tableIdStr.split("\\.")[0].toUpperCase(),
                        tableIdStr.split("\\.")[1].toUpperCase());
        Schema schema = createTableEvent.getSchema();
        fieldGetterMaps.put(tableId2, SchemaUtils.createFieldGetters(schema));
        StringBuilder actualBinlogStr = getResultString(actual, fieldGetterMaps, true);
        StringBuilder expectedBinlogStr = getResultString(expectedBinlog, fieldGetterMaps, true);
        assertThat(actualBinlogStr.toString()).isEqualTo(expectedBinlogStr.toString());
        for (int i = 0; i < expectedBinlog.size(); i++) {
            if (actual.get(i) instanceof DataChangeEvent) {
                assertThat(Long.parseLong(((DataChangeEvent) actual.get(i)).meta().get("op_ts")))
                        .isGreaterThanOrEqualTo(
                                Long.parseLong(
                                        ((DataChangeEvent) expectedBinlog.get(i))
                                                .meta()
                                                .get("op_ts")));
            }
        }
    }

    private CreateTableEvent getProductsCreateTableEvent(TableId tableId) {
        return new CreateTableEvent(
                tableId,
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .physicalColumn("name", DataTypes.VARCHAR(255).notNull())
                        .physicalColumn("description", DataTypes.VARCHAR(512))
                        .physicalColumn("weight", DataTypes.FLOAT())
                        .primaryKey(Collections.singletonList("id"))
                        .build());
    }

    private List<Event> getSnapshotExpected(TableId tableId) {

        RowType rowType =
                RowType.of(
                        new DataType[] {
                            DataTypes.BIGINT().notNull(),
                            DataTypes.VARCHAR(255).notNull(),
                            DataTypes.VARCHAR(512),
                            DataTypes.FLOAT()
                        },
                        new String[] {"ID", "NAME", "DESCRIPTION", "WEIGHT"});
        BinaryRecordDataGenerator generator = new BinaryRecordDataGenerator(rowType);

        List<Event> snapshotExpected = new ArrayList<>();
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    101L,
                                    BinaryStringData.fromString("scooter"),
                                    BinaryStringData.fromString("Small 2-wheel scooter"),
                                    3.14f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    102L,
                                    BinaryStringData.fromString("car battery"),
                                    BinaryStringData.fromString("12V car battery"),
                                    8.1f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    103L,
                                    BinaryStringData.fromString("12-pack drill bits"),
                                    BinaryStringData.fromString(
                                            "12-pack of drill bits with sizes ranging from #40 to #3"),
                                    0.8f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    104L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("12oz carpenters hammer"),
                                    0.75f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    105L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("14oz carpenters hammer"),
                                    0.875f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    106L,
                                    BinaryStringData.fromString("hammer"),
                                    BinaryStringData.fromString("16oz carpenters hammer"),
                                    1.0f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    107L,
                                    BinaryStringData.fromString("rocks"),
                                    BinaryStringData.fromString("box of assorted rocks"),
                                    5.3f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    108L,
                                    BinaryStringData.fromString("jacket"),
                                    BinaryStringData.fromString(
                                            "water resistent black wind breaker"),
                                    0.1f
                                })));
        snapshotExpected.add(
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    109L,
                                    BinaryStringData.fromString("spare tire"),
                                    BinaryStringData.fromString("24 inch spare tire"),
                                    22.2f
                                })));
        return snapshotExpected;
    }

    /**
     * * The final schema of table products is as follows.
     *
     * <pre>
     * CREATE TABLE products (
     *   id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
     *   name VARCHAR(255) NOT NULL DEFAULT 'flink',
     *   weight FLOAT,
     *   col1 VARCHAR(45),
     *   col2 VARCHAR(55)
     * );
     * </pre>
     */
    private List<Event> executeAlterAndProvideExpected(TableId tableId, Statement statement)
            throws SQLException {
        List<Event> expected = new ArrayList<>();
        statement.execute(
                String.format(
                        "alter table %s.products modify DESCRIPTION VARCHAR(255) DEFAULT NULL",
                        "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products RENAME COLUMN DESCRIPTION TO DESCPT", "debezium"));

        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESCRIPTION", DataTypes.VARCHAR(255))));
        expected.add(
                new RenameColumnEvent(tableId, Collections.singletonMap("DESCRIPTION", "DESCPT")));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products MODIFY DESCPT VARCHAR(400) DEFAULT NULL",
                        "debezium"));
        statement.execute(
                String.format("ALTER TABLE %s.products RENAME COLUMN DESCPT TO DESC2", "debezium"));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESCPT", DataTypes.VARCHAR(400))));
        expected.add(new RenameColumnEvent(tableId, Collections.singletonMap("DESCPT", "DESC2")));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD DESC1 VARCHAR(45) DEFAULT NULL", "debezium"));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("DESC1", DataTypes.VARCHAR(45))))));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD COL1 VARCHAR(45) DEFAULT NULL", "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products ADD COL2 VARCHAR(55) DEFAULT NULL", "debezium"));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("COL1", DataTypes.VARCHAR(45))))));
        expected.add(
                new AddColumnEvent(
                        tableId,
                        Collections.singletonList(
                                new AddColumnEvent.ColumnWithPosition(
                                        Column.physicalColumn("COL2", DataTypes.VARCHAR(55))))));

        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN DESC1", "debezium"));
        statement.execute(
                String.format(
                        "ALTER TABLE %s.products MODIFY DESC2 VARCHAR(512) DEFAULT NULL",
                        "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("DESC1")));
        expected.add(
                new AlterColumnTypeEvent(
                        tableId, Collections.singletonMap("DESC2", DataTypes.VARCHAR(512))));

        statement.execute(
                String.format(
                        "ALTER TABLE %s.products RENAME COLUMN DESC2 TO DESCRIPTION", "debezium"));
        expected.add(
                new RenameColumnEvent(tableId, Collections.singletonMap("DESC2", "DESCRIPTION")));

        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN COL1", "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("COL1")));
        statement.execute(String.format("ALTER TABLE %s.products DROP COLUMN COL2", "debezium"));
        expected.add(new DropColumnEvent(tableId, Collections.singletonList("COL2")));

        // Should not catch SchemaChangeEvent of tables other than products
        statement.execute(
                String.format(
                        "ALTER TABLE %s.category ADD DESC1 VARCHAR(45) DEFAULT NULL", "debezium"));
        statement.execute(String.format("ALTER TABLE %s.category DROP COLUMN DESC1", "debezium"));
        return expected;
    }

    public static <T> List<T> fetchResults(Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            result.add(event);
            size--;
        }
        return result;
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
