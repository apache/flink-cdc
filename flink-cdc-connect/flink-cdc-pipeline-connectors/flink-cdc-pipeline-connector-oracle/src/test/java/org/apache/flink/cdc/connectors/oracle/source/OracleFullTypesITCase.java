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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.LocalZonedTimestampData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.oracle.factory.OracleDataSourceFactory;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfigFactory;
import org.apache.flink.cdc.connectors.oracle.table.OracleReadableMetaData;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.math.BigDecimal;
import java.sql.Connection;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.METADATA_LIST;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Oracle event source. */
public class OracleFullTypesITCase extends OracleSourceTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting oracle19c containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Container oracle19c is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping oracle19c containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Container oracle19c is stopped.");
    }

    @BeforeEach
    public void before() throws Exception {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(200);
        env.setRestartStrategy(RestartStrategies.noRestart());
        Connection conn = getJdbcConnectionAsDBA();
        conn.createStatement().execute("GRANT ANALYZE ANY TO " + CONNECTOR_USER);
    }

    @Test
    public void testOracleCommonDataTypes() throws Throwable {
        testCommonDataTypes();
    }

    private void testCommonDataTypes() throws Exception {
        createAndInitialize("column_type_test.sql");
        CloseableIterator<Event> iterator =
                env.fromSource(
                                getFlinkSourceProvider(new String[] {"FULL_TYPES"}).getSource(),
                                WatermarkStrategy.noWatermarks(),
                                "Event-Source")
                        .executeAndCollect();

        Object[] expectedSnapshot =
                new Object[] {
                    DecimalData.fromBigDecimal(new BigDecimal("1"), 9, 0),
                    BinaryStringData.fromString("vc2"),
                    BinaryStringData.fromString("vc2"),
                    BinaryStringData.fromString("nvc2"),
                    BinaryStringData.fromString("c  "),
                    BinaryStringData.fromString("nc "),
                    (float) 1.1,
                    2.22,
                    (float) 3.33,
                    (float) 8.888,
                    DecimalData.fromBigDecimal(new BigDecimal("4.4444"), 10, 6),
                    (float) 5.555,
                    (float) 6.66,
                    DecimalData.fromBigDecimal(new BigDecimal("1234.567891"), 10, 6),
                    DecimalData.fromBigDecimal(new BigDecimal("1234.567891"), 10, 6),
                    DecimalData.fromBigDecimal(new BigDecimal("77.323"), 10, 3),
                    DecimalData.fromBigDecimal(new BigDecimal(1), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(22), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(333), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(4444), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(5555), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(1), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(99), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(9999), 10, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(999999999), 38, 0),
                    DecimalData.fromBigDecimal(new BigDecimal(999999999999999999L), 38, 0),
                    90L,
                    9900L,
                    999999990L,
                    999999999999999900L,
                    BinaryStringData.fromString("9.99999999999999999999999999999999999E+37"),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 00:00:00",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 12:34:56.00789",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 12:34:56.13",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"))),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 12:34:56.1255",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"))),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 12:34:56.125457",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(
                                    "2022-10-30 12:34:56.00789",
                                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))),
                    LocalZonedTimestampData.fromInstant(
                            LocalDateTime.parse(
                                            "2022-10-30 01:34:56.00789",
                                            DateTimeFormatter.ofPattern(
                                                    "yyyy-MM-dd HH:mm:ss.SSSSS"))
                                    .atZone(ZoneId.of("Asia/Shanghai"))
                                    .toInstant()),
                    -110451600000000L,
                    -93784560000L,
                    BinaryStringData.fromString(
                            "<name>\n  <a id=\"1\" value=\"some values\">test xmlType</a>\n</name>\n")
                };
        // skip CreateTableEvent
        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(iterator, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();
        Object[] objects = recordFields(snapshotRecord, COMMON_TYPES);
        assertThat(objects).isEqualTo(expectedSnapshot);
    }

    private FlinkSourceProvider getFlinkSourceProvider(String[] captureTables) {
        String[] captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> "DEBEZIUM." + tableName)
                        .toArray(String[]::new);

        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "DEBEZIUM\\.FULL_TYPES");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        options.put(METADATA_LIST.key(), "op_ts");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        OracleSourceConfigFactory configFactory = new OracleSourceConfigFactory();
        configFactory.username(CONNECTOR_USER);
        configFactory.password(CONNECTOR_PWD);
        configFactory.port(ORACLE_CONTAINER.getOraclePort());
        configFactory.databaseList(ORACLE_CONTAINER.getDatabaseName());
        configFactory.hostname(ORACLE_CONTAINER.getHost());
        configFactory.tableList("DEBEZIUM\\.FULL_TYPES");
        List<OracleReadableMetaData> readableMetadataList =
                OracleDataSourceFactory.listReadableMetadata("op_ts");
        return (FlinkSourceProvider)
                new OracleDataSource(
                                configFactory,
                                context.getFactoryConfiguration(),
                                readableMetadataList)
                        .getEventSourceProvider();
    }

    private static final RowType COMMON_TYPES =
            RowType.of(
                    DataTypes.DECIMAL(9, 0).notNull(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.STRING(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.DECIMAL(10, 6),
                    DataTypes.FLOAT(),
                    DataTypes.FLOAT(),
                    DataTypes.DECIMAL(10, 6),
                    DataTypes.DECIMAL(10, 6),
                    DataTypes.DECIMAL(10, 3),
                    DataTypes.DECIMAL(10, 0),
                    DataTypes.DECIMAL(10, 0),
                    DataTypes.DECIMAL(10, 0),
                    DataTypes.DECIMAL(38, 0),
                    DataTypes.DECIMAL(38, 0),
                    DataTypes.DECIMAL(1, 0),
                    DataTypes.DECIMAL(2, 0),
                    DataTypes.DECIMAL(4, 0),
                    DataTypes.DECIMAL(9, 0),
                    DataTypes.DECIMAL(18, 0),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.STRING(),
                    DataTypes.TIMESTAMP(),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP(2),
                    DataTypes.TIMESTAMP(4),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.TIMESTAMP_LTZ(6),
                    DataTypes.BIGINT(),
                    DataTypes.BIGINT(),
                    DataTypes.STRING());

    public static <T> Tuple2<List<T>, List<CreateTableEvent>> fetchResultsAndCreateTableEvent(
            Iterator<T> iter, int size) {
        List<T> result = new ArrayList<>(size);
        List<CreateTableEvent> createTableEvents = new ArrayList<>();
        while (size > 0 && iter.hasNext()) {
            T event = iter.next();
            if (event instanceof CreateTableEvent) {
                createTableEvents.add((CreateTableEvent) event);
            } else {
                result.add(event);
                size--;
            }
        }
        return Tuple2.of(result, createTableEvents);
    }

    public static Object[] recordFields(RecordData record, RowType rowType) {
        int fieldNum = record.getArity();
        List<DataType> fieldTypes = rowType.getChildren();
        Object[] fields = new Object[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            if (record.isNullAt(i)) {
                fields[i] = null;
            } else {
                DataType type = fieldTypes.get(i);
                RecordData.FieldGetter fieldGetter;
                fieldGetter = RecordData.createFieldGetter(type, i);
                Object o = fieldGetter.getFieldOrNull(record);
                fields[i] = o;
            }
        }
        return fields;
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
