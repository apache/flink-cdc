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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.runtime.partitioning.PartitioningEvent;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Some common schema testing utilities & functions. */
public abstract class SchemaTestBase {
    protected static final List<RouteRule> ROUTING_RULES =
            Arrays.asList(
                    // Simple 1-to-1 routing rules
                    new RouteRule("db_1.table_1", "db_1.table_1"),
                    new RouteRule("db_1.table_2", "db_1.table_2"),
                    new RouteRule("db_1.table_3", "db_1.table_3"),

                    // Re-routed rules
                    new RouteRule("db_2.table_1", "db_2.table_2"),
                    new RouteRule("db_2.table_2", "db_2.table_3"),
                    new RouteRule("db_2.table_3", "db_2.table_1"),

                    // Merging tables
                    new RouteRule("db_3.table_\\.*", "db_3.table_merged"),

                    // Broadcast tables
                    new RouteRule("db_4.table_1", "db_4.table_a"),
                    new RouteRule("db_4.table_1", "db_4.table_b"),
                    new RouteRule("db_4.table_1", "db_4.table_c"),
                    new RouteRule("db_4.table_2", "db_4.table_b"),
                    new RouteRule("db_4.table_2", "db_4.table_c"),
                    new RouteRule("db_4.table_3", "db_4.table_c"),

                    // RepSym routes
                    new RouteRule("db_5.table_\\.*", "db_5.prefix_<>_suffix", "<>"),

                    // Irrelevant routes
                    new RouteRule("foo", "bar", null));

    protected static final TableIdRouter TABLE_ID_ROUTER = new TableIdRouter(ROUTING_RULES);

    protected static BinaryRecordData genBinRec(String rowType, Object... fields) {
        return (new BinaryRecordDataGenerator(quickGenRow(rowType).toArray(new DataType[0])))
                .generate(
                        Arrays.stream(fields)
                                .map(
                                        e ->
                                                (e instanceof String)
                                                        ? BinaryStringData.fromString((String) e)
                                                        : e)
                                .toArray());
    }

    protected static List<DataType> quickGenRow(String crypticExpr) {
        List<DataType> rowTypes = new ArrayList<>(crypticExpr.length());
        for (char c : crypticExpr.toLowerCase().toCharArray()) {
            switch (c) {
                case 'b':
                    rowTypes.add(DataTypes.BOOLEAN());
                    break;
                case 'i':
                    rowTypes.add(DataTypes.INT());
                    break;
                case 'l':
                    rowTypes.add(DataTypes.BIGINT());
                    break;
                case 'f':
                    rowTypes.add(DataTypes.FLOAT());
                    break;
                case 'd':
                    rowTypes.add(DataTypes.DOUBLE());
                    break;
                case 's':
                    rowTypes.add(DataTypes.STRING());
                    break;
                default:
                    throw new IllegalStateException("Unexpected type char: " + c);
            }
        }
        return rowTypes;
    }

    protected static DataChangeEvent genInsert(TableId tableId, String rowType, Object... fields) {
        return DataChangeEvent.insertEvent(tableId, genBinRec(rowType, fields));
    }

    protected static StreamRecord<PartitioningEvent> wrap(Event payload) {
        return wrap(payload, 0, 0);
    }

    protected static StreamRecord<PartitioningEvent> wrap(Event payload, int from, int to) {
        return new StreamRecord<>(PartitioningEvent.ofDistributed(payload, from, to));
    }
}
