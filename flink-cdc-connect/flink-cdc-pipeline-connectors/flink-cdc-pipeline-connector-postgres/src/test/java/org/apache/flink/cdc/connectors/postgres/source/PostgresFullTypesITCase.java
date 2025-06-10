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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cdc.common.data.DecimalData;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.TimestampData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.factory.PostgresDataSourceFactory;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

/** Test cases for {@link PostgresTestBase} with full types. */
public class PostgresFullTypesITCase extends PostgresTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresFullTypesITCase.class);

    /** Use postgis plugin to test the GIS type. */
    protected static final DockerImageName POSTGIS_IMAGE =
            DockerImageName.parse("postgis/postgis:14-3.5").asCompatibleSubstituteFor("postgres");

    public static final PostgreSQLContainer<?> POSTGIS_CONTAINER =
            new PostgreSQLContainer<>(POSTGIS_IMAGE)
                    .withDatabaseName(DEFAULT_DB)
                    .withUsername("postgres")
                    .withPassword("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withCommand(
                            "postgres",
                            "-c",
                            // default
                            "fsync=off",
                            "-c",
                            "max_replication_slots=20",
                            "-c",
                            "wal_level=logical");

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    @BeforeAll
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(POSTGIS_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    public static void stopContainers() {
        LOG.info("Stopping containers...");
        POSTGIS_CONTAINER.stop();
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(4);
        env.enableCheckpointing(2000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testFullTypes() throws Exception {
        initializePostgresTable(POSTGIS_CONTAINER, "column_type_test");
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname(POSTGIS_CONTAINER.getHost())
                                .port(POSTGIS_CONTAINER.getMappedPort(POSTGRESQL_PORT))
                                .username(TEST_USER)
                                .password(TEST_PASSWORD)
                                .databaseList(POSTGRES_CONTAINER.getDatabaseName())
                                .tableList("inventory.full_types")
                                .startupOptions(StartupOptions.initial())
                                .serverTimeZone("UTC");
        configFactory.database(POSTGRES_CONTAINER.getDatabaseName());
        configFactory.slotName(getSlotName());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider)
                        new PostgresDataSource(configFactory).getEventSourceProvider();

        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                PostgresDataSourceFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();

        Object[] expectedSnapshot =
                new Object[] {
                    1,
                    new byte[] {50},
                    (short) 32767,
                    65535,
                    2147483647L,
                    DecimalData.fromBigDecimal(new BigDecimal("5.5"), 2, 1)
                            .toBigDecimal()
                            .floatValue(),
                    DecimalData.fromBigDecimal(new BigDecimal("6.6"), 2, 1)
                            .toBigDecimal()
                            .doubleValue(),
                    DecimalData.fromBigDecimal(new BigDecimal("123.12345"), 10, 5),
                    DecimalData.fromBigDecimal(new BigDecimal("404.4"), 5, 1),
                    true,
                    BinaryStringData.fromString("Hello World"),
                    BinaryStringData.fromString("a"),
                    BinaryStringData.fromString("abc"),
                    BinaryStringData.fromString("abcd..xyz"),
                    TimestampData.fromLocalDateTime(LocalDateTime.parse("2020-07-17T18:00:22.123")),
                    TimestampData.fromLocalDateTime(
                            LocalDateTime.parse("2020-07-17T18:00:22.123456")),
                    18460,
                    64822000,
                    DecimalData.fromBigDecimal(new BigDecimal("500"), 10, 0)
                };

        List<Event> snapshotResults = fetchResultsAndCreateTableEvent(events, 1).f0;
        RecordData snapshotRecord = ((DataChangeEvent) snapshotResults.get(0)).after();

        Assertions.assertThat(recordFields(snapshotRecord, PG_TYPES)).isEqualTo(expectedSnapshot);
    }

    private <T> Tuple2<List<T>, List<CreateTableEvent>> fetchResultsAndCreateTableEvent(
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

    private Object[] recordFields(RecordData record, RowType rowType) {
        int fieldNum = record.getArity();
        List<DataType> fieldTypes = rowType.getChildren();
        Object[] fields = new Object[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            if (record.isNullAt(i)) {
                fields[i] = null;
            } else {
                DataType type = fieldTypes.get(i);
                RecordData.FieldGetter fieldGetter = RecordData.createFieldGetter(type, i);
                Object o = fieldGetter.getFieldOrNull(record);
                fields[i] = o;
            }
        }
        return fields;
    }

    private static final RowType PG_TYPES =
            RowType.of(
                    DataTypes.INT(),
                    DataTypes.BYTES(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.DECIMAL(10, 5),
                    DataTypes.DECIMAL(10, 1),
                    DataTypes.BOOLEAN(),
                    DataTypes.STRING(),
                    DataTypes.CHAR(1),
                    DataTypes.CHAR(3),
                    DataTypes.VARCHAR(20),
                    DataTypes.TIMESTAMP(3),
                    DataTypes.TIMESTAMP(6),
                    DataTypes.DATE(),
                    DataTypes.TIME(0),
                    DataTypes.DECIMAL(DecimalType.DEFAULT_PRECISION, DecimalType.DEFAULT_SCALE));
}
