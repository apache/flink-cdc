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

package org.apache.flink.cdc.connectors.values.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.RowType;
import org.apache.flink.cdc.connectors.values.ValuesDatabase;
import org.apache.flink.cdc.connectors.values.factory.ValuesDataFactory;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for {@link org.apache.flink.cdc.connectors.values.source.ValuesDataSource} in
 * different enumeration situations of {@link
 * org.apache.flink.cdc.connectors.values.source.ValuesDataSourceHelper}.
 */
class ValuesDataSourceITCase {

    @BeforeEach
    public void before() {
        ValuesDatabase.clear();
    }

    @AfterEach
    public void after() {
        ValuesDatabase.clear();
    }

    /**
     * read Events from {@link org.apache.flink.cdc.connectors.values.source.ValuesDataSource} and
     * apply the events to ValuesDatabase.
     */
    private void executeDataStreamJob(ValuesDataSourceHelper.EventSetId type) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) new ValuesDataSource(type).getEventSourceProvider();
        CloseableIterator<Event> events =
                env.fromSource(
                                sourceProvider.getSource(),
                                WatermarkStrategy.noWatermarks(),
                                ValuesDataFactory.IDENTIFIER,
                                new EventTypeInfo())
                        .executeAndCollect();
        events.forEachRemaining(
                (event) -> {
                    if (event instanceof DataChangeEvent) {
                        ValuesDatabase.applyDataChangeEvent((DataChangeEvent) event);
                    } else if (event instanceof SchemaChangeEvent) {
                        ValuesDatabase.applySchemaChangeEvent((SchemaChangeEvent) event);
                    }
                });
    }

    @Test
    void testSingleSplitSingleTable() throws Exception {
        executeDataStreamJob(ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_SINGLE_TABLE);
        List<String> results = new ArrayList<>();
        results.add("default_namespace.default_schema.table1:col1=2;newCol3=x");
        results.add("default_namespace.default_schema.table1:col1=3;newCol3=");
        Assertions.assertThat(
                        ValuesDatabase.getResults(
                                TableId.parse("default_namespace.default_schema.table1")))
                .isEqualTo(results);
    }

    @Test
    void testSingleSplitMultiTables() throws Exception {
        executeDataStreamJob(ValuesDataSourceHelper.EventSetId.SINGLE_SPLIT_MULTI_TABLES);
        List<String> results = new ArrayList<>();
        results.add("default_namespace.default_schema.table1:col1=2;newCol3=x");
        results.add("default_namespace.default_schema.table1:col1=3;newCol3=");
        Assertions.assertThat(
                        ValuesDatabase.getResults(
                                TableId.parse("default_namespace.default_schema.table1")))
                .isEqualTo(results);

        results.clear();
        results.add("default_namespace.default_schema.table2:col1=1;col2=1");
        results.add("default_namespace.default_schema.table2:col1=2;col2=2");
        results.add("default_namespace.default_schema.table2:col1=3;col2=3");
        Assertions.assertThat(
                        ValuesDatabase.getResults(
                                TableId.parse("default_namespace.default_schema.table2")))
                .isEqualTo(results);
    }

    @Test
    void testCustomSourceEvents() throws Exception {
        List<List<Event>> splits = new ArrayList<>();
        List<Event> split1 = new ArrayList<>();
        TableId table1 = TableId.tableId("default_namespace", "default_schema", "table1");
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("col1", DataTypes.STRING())
                        .physicalColumn("col2", DataTypes.STRING())
                        .primaryKey("col1")
                        .build();
        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(DataTypes.STRING(), DataTypes.STRING()));
        CreateTableEvent createTableEvent = new CreateTableEvent(table1, schema);
        split1.add(createTableEvent);
        DataChangeEvent insertEvent1 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("1"),
                                    BinaryStringData.fromString("1")
                                }));
        split1.add(insertEvent1);
        DataChangeEvent insertEvent2 =
                DataChangeEvent.insertEvent(
                        table1,
                        generator.generate(
                                new Object[] {
                                    BinaryStringData.fromString("2"),
                                    BinaryStringData.fromString("2")
                                }));
        split1.add(insertEvent2);
        splits.add(split1);
        ValuesDataSourceHelper.setSourceEvents(splits);
        executeDataStreamJob(ValuesDataSourceHelper.EventSetId.CUSTOM_SOURCE_EVENTS);

        List<String> results = new ArrayList<>();
        results.add("default_namespace.default_schema.table1:col1=1;col2=1");
        results.add("default_namespace.default_schema.table1:col1=2;col2=2");
        Assertions.assertThat(
                        ValuesDatabase.getResults(
                                TableId.parse("default_namespace.default_schema.table1")))
                .isEqualTo(results);
    }

    @Test
    void testMultiSplitsSingleTable() throws Exception {
        executeDataStreamJob(ValuesDataSourceHelper.EventSetId.MULTI_SPLITS_SINGLE_TABLE);
        List<String> results = new ArrayList<>();
        results.add("default_namespace.default_schema.table1:col1=1;col2=1;col3=x");
        results.add("default_namespace.default_schema.table1:col1=3;col2=3;col3=x");
        results.add("default_namespace.default_schema.table1:col1=5;col2=5;col3=");
        Assertions.assertThat(
                        ValuesDatabase.getResults(
                                TableId.parse("default_namespace.default_schema.table1")))
                .isEqualTo(results);
    }
}
