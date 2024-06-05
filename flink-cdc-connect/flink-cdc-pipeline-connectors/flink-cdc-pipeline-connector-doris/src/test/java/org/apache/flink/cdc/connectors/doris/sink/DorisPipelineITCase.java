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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.FlinkSinkProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.doris.sink.utils.DorisContainer;
import org.apache.flink.cdc.connectors.doris.sink.utils.DorisSinkTestBase;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.BENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.FENODES;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_BATCH_MODE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.SINK_ENABLE_DELETE;
import static org.apache.flink.cdc.connectors.doris.sink.DorisDataSinkOptions.USERNAME;

/** IT tests for {@link DorisDataSink}. */
public class DorisPipelineITCase extends DorisSinkTestBase {

    private static final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final int DATABASE_OPERATION_TIMEOUT_SECONDS = 5;

    @BeforeClass
    public static void before() {
        env.setParallelism(DEFAULT_PARALLELISM);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
    }

    @Before
    public void initializeDatabaseAndTable() {
        createDatabase(DorisContainer.DORIS_DATABASE_NAME);

        // waiting for table to be created
        DORIS_CONTAINER.waitForLog(
                String.format(".*createDb dbName = %s,.*\\s", DorisContainer.DORIS_DATABASE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Database {} created.", DorisContainer.DORIS_DATABASE_NAME);

        createTable(
                DorisContainer.DORIS_DATABASE_NAME,
                DorisContainer.DORIS_TABLE_NAME,
                "id",
                Arrays.asList("id INT NOT NULL", "number DOUBLE", "name VARCHAR(51)"));

        // waiting for table to be created
        DORIS_CONTAINER.waitForLog(
                String.format(
                        ".*successfully create table\\[%s;.*\\s", DorisContainer.DORIS_TABLE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Table {} created.", DorisContainer.DORIS_TABLE_NAME);
    }

    @After
    public void destroyDatabaseAndTable() {
        dropTable(DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);
        // waiting for table to be dropped
        DORIS_CONTAINER.waitForLog(
                String.format(
                        ".*finished dropping table: %s.*\\s", DorisContainer.DORIS_TABLE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Table {} destroyed.", DorisContainer.DORIS_TABLE_NAME);

        dropDatabase(DorisContainer.DORIS_DATABASE_NAME);
        // waiting for database to be created
        DORIS_CONTAINER.waitForLog(
                String.format(
                        ".*finish drop database\\[%s\\].*\\s", DorisContainer.DORIS_DATABASE_NAME),
                1,
                DATABASE_OPERATION_TIMEOUT_SECONDS);

        LOG.info("Database {} destroyed.", DorisContainer.DORIS_DATABASE_NAME);
    }

    @Test
    public void testDorisSinkStreamJob() throws Exception {
        runValuesToDorisJob(false);
    }

    @Test
    public void testDorisSinkBatchJob() throws Exception {
        runValuesToDorisJob(true);
    }

    private List<Event> generateEvents(TableId tableId) {
        Schema schema =
                Schema.newBuilder()
                        .column(new PhysicalColumn("id", DataTypes.INT().notNull(), null))
                        .column(new PhysicalColumn("number", DataTypes.DOUBLE(), null))
                        .column(new PhysicalColumn("name", DataTypes.VARCHAR(17), null))
                        .primaryKey("id")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(
                        RowType.of(DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.VARCHAR(17)));

        return Arrays.asList(
                new CreateTableEvent(tableId, schema),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {17, 3.14, BinaryStringData.fromString("Doris Day")})),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19, 2.718, BinaryStringData.fromString("Que Sera Sera")
                                })),
                DataChangeEvent.insertEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    21, 1.732, BinaryStringData.fromString("Disenchanted")
                                })),
                DataChangeEvent.updateEvent(
                        tableId,
                        generator.generate(
                                new Object[] {17, 3.14, BinaryStringData.fromString("Doris Day")}),
                        generator.generate(
                                new Object[] {17, 6.28, BinaryStringData.fromString("Doris Day")})),
                DataChangeEvent.deleteEvent(
                        tableId,
                        generator.generate(
                                new Object[] {
                                    19, 2.718, BinaryStringData.fromString("Que Sera Sera")
                                })));
    }

    private void runValuesToDorisJob(boolean batchMode) throws Exception {
        TableId tableId =
                TableId.tableId(
                        DorisContainer.DORIS_DATABASE_NAME, DorisContainer.DORIS_TABLE_NAME);

        DataStream<Event> stream =
                env.fromCollection(generateEvents(tableId), TypeInformation.of(Event.class));

        Configuration config =
                new Configuration()
                        .set(FENODES, DORIS_CONTAINER.getFeNodes())
                        .set(BENODES, DORIS_CONTAINER.getBeNodes())
                        .set(USERNAME, DorisContainer.DORIS_USERNAME)
                        .set(PASSWORD, DorisContainer.DORIS_PASSWORD)
                        .set(SINK_ENABLE_BATCH_MODE, batchMode)
                        .set(SINK_ENABLE_DELETE, true);

        config.addAll(
                Configuration.fromMap(
                        Collections.singletonMap("table.create.properties.replication_num", "1")));

        Sink<Event> dorisSink =
                ((FlinkSinkProvider) createDorisDataSink(config).getEventSinkProvider()).getSink();

        stream.sinkTo(dorisSink);

        env.execute("Values to Doris Sink");

        List<String> actual = fetchTableContent(tableId, 3);

        List<String> expected = Arrays.asList("17 | 6.28 | Doris Day", "21 | 1.732 | Disenchanted");

        assertEqualsInAnyOrder(expected, actual);
    }
}
