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

package com.apache.flink.cdc.connectors.oracle.source.reader;

import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceConfig;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.cdc.debezium.Validator;
import org.apache.flink.cdc.debezium.internal.DebeziumOffset;

import com.apache.flink.cdc.connectors.oracle.utils.DebeziumUtils;
import io.debezium.engine.DebeziumEngine;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.apache.flink.cdc.connectors.oracle.utils.OracleSchemaUtils.getSchema;

/**
 * The {@link OracleDebeziumSourceFunction} is a streaming data source that pulls captured change
 * data from databases into Flink.
 *
 * <p>There are two workers during the runtime. One worker periodically pulls records from the
 * database and pushes the records into the {@link Handover}. The other worker consumes the records
 * from the {@link Handover} and convert the records to the data in Flink style. The reason why
 * don't use one workers is because debezium has different behaviours in snapshot phase and
 * streaming phase.
 *
 * <p>Here we use the {@link Handover} as the buffer to submit data from the producer to the
 * consumer. Because the two threads don't communicate to each other directly, the error reporting
 * also relies on {@link Handover}. When the engine gets errors, the engine uses the {@link
 * DebeziumEngine.CompletionCallback} to report errors to the {@link Handover} and wakes up the
 * consumer to check the error. However, the source function just closes the engine and wakes up the
 * producer if the error is from the Flink side.
 *
 * <p>If the execution is canceled or finish(only snapshot phase), the exit logic is as same as the
 * logic in the error reporting.
 *
 * <p>The source function participates in checkpointing and guarantees that no data is lost during a
 * failure, and that the computation processes elements "exactly once".
 *
 * <p>Note: currently, the source function can't run in multiple parallel instances.
 *
 * <p>Please refer to Debezium's documentation for the available configuration properties:
 * https://debezium.io/documentation/reference/1.9/development/engine.html#engine-properties
 */
@PublicEvolving
public class OracleDebeziumSourceFunction<T> extends DebeziumSourceFunction<T> {
    private Set<TableId> alreadySendCreateTableTables;
    private String[] tableList;
    private OracleSourceConfig sourceConfig;

    public OracleDebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator,
            String[] tableList,
            OracleSourceConfig sourceConfig) {
        super(deserializer, properties, specificOffset, validator);
        this.tableList = tableList;
        this.sourceConfig = sourceConfig;
        alreadySendCreateTableTables = new HashSet<>();
    }

    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

        Arrays.stream(tableList)
                .sequential()
                .forEach(
                        e -> {
                            TableId tableId = TableId.parse(e);
                            if (!alreadySendCreateTableTables.contains(tableId)) {
                                try (JdbcConnection jdbc =
                                        DebeziumUtils.createOracleConnection(sourceConfig)) {
                                    sendCreateTableEvent(
                                            jdbc, tableId, (SourceContext<Event>) sourceContext);
                                    alreadySendCreateTableTables.add(tableId);
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                }
                            }
                        });
        super.run(sourceContext);
    }

    private void sendCreateTableEvent(
            JdbcConnection jdbc, TableId tableId, SourceContext<Event> sourceContext) {
        Schema schema = getSchema(jdbc, tableId);
        sourceContext.collect(
                new CreateTableEvent(
                        org.apache.flink.cdc.common.event.TableId.tableId(
                                tableId.catalog(), tableId.table()),
                        schema));
    }
}
