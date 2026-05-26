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

package org.apache.flink.cdc.connectors.postgres.source.reader;

import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for the {@link PostgresSourceReader}. */
public class PostgresSourceReaderITCase extends PostgresTestBase {

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    void before() throws SQLException {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    void after() throws Exception {
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @Test
    public void testReceiveLogicalMessagesWithPrefix() throws Exception {
        PostgresSourceBuilder.PostgresIncrementalSource<String> source =
                PostgresSourceBuilder.<String>builder()
                        .hostname(POSTGRES_CONTAINER.getHost())
                        .port(POSTGRES_CONTAINER.getPostgreSQLPort())
                        .database(customDatabase.getDatabaseName())
                        .schemaList(customDatabase.getSchemaName())
                        .tableList(customDatabase.getSchemaName() + ".customers")
                        .username(POSTGRES_CONTAINER.getUsername())
                        .password(POSTGRES_CONTAINER.getPassword())
                        .decodingPluginName("pgoutput")
                        .slotName(slotName)
                        .logicalMessageEnabled(true)
                        .logicalMessagePrefixes(Collections.singletonList("test_prefix"))
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final CountDownLatch latch = new CountDownLatch(2);
        final List<String> results = Collections.synchronizedList(Arrays.asList(new String[2]));

        env.addSource((SourceFunction<String>) source)
                .addSink(
                        new org.apache.flink.streaming.api.functions.sink.SinkFunction<String>() {
                            private int index = 0;

                            @Override
                            public void invoke(String value, Context context) throws Exception {
                                if (index < 2) {
                                    results.set(index, value);
                                }
                                index++;
                                latch.countDown();
                            }
                        });

        Thread thread =
                new Thread(
                        () -> {
                            try {
                                env.execute("test-logical-messages");
                            } catch (Exception e) {
                                // ignore
                            }
                        });
        thread.start();

        // wait for the source to start
        Thread.sleep(1000);

        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, customDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_logical_emit_message(false, 'test_prefix', 'message1')");
            stmt.execute("SELECT pg_logical_emit_message(false, 'other_prefix', 'message2')");
            stmt.execute("SELECT pg_logical_emit_message(false, 'test_prefix', 'message3')");
        }

        latch.await(5, TimeUnit.SECONDS);

        assertThat(results)
                .anySatisfy(
                        s -> {
                            assertThat(s).contains("\"op\":\"m\"");
                            assertThat(s).contains("\"message_prefix\":\"test_prefix\"");
                            assertThat(s).contains("\"message_content\":\"message1\"");
                        })
                .anySatisfy(
                        s -> {
                            assertThat(s).contains("\"op\":\"m\"");
                            assertThat(s).contains("\"message_prefix\":\"test_prefix\"");
                            assertThat(s).contains("\"message_content\":\"message3\"");
                        })
                .noneSatisfy(
                        s -> {
                            assertThat(s).contains("\"message_prefix\":\"other_prefix\"");
                        });

        thread.interrupt();
    }
}
