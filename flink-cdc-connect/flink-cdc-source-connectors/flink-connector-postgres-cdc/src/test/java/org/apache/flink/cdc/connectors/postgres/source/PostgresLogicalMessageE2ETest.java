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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.postgres.PostgresTestBase;
import org.apache.flink.cdc.connectors.postgres.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** E2E tests for logical message filtering in the Postgres CDC source. */
public class PostgresLogicalMessageE2ETest extends PostgresTestBase {

    @RegisterExtension
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                            .withHaLeadershipControl()
                            .build());

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    "postgres",
                    "customer",
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    private String slotName;

    @BeforeEach
    public void before() {
        customDatabase.createAndInitialize();
        this.slotName = getSlotName();
    }

    @AfterEach
    public void after() throws Exception {
        Thread.sleep(1000L);
        customDatabase.removeSlot(slotName);
    }

    @Test
    public void testLogicalMessageFiltering() throws Exception {
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
                        .deserializer(new LogicalMessageDeserializer())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final CountDownLatch latch = new CountDownLatch(2); // only 2 messages should be received
        final List<String> results = Collections.synchronizedList(new ArrayList<>());

        Thread producerThread =
                new Thread(
                        () -> {
                            try (CloseableIterator<String> iterator =
                                    env.fromSource(
                                                    source,
                                                    WatermarkStrategy.noWatermarks(),
                                                    "Postgres CDC Source")
                                            .executeAndCollect()) {
                                while (iterator.hasNext()) {
                                    String next = iterator.next();
                                    if (next.contains("op\":\"m\"")) {
                                        results.add(next);
                                        latch.countDown();
                                    }
                                }
                            } catch (Exception e) {
                                // ignore
                            }
                        });
        producerThread.start();

        // Wait for the source to start and read the snapshot
        Thread.sleep(5000);

        try (Connection conn =
                        getJdbcConnection(POSTGRES_CONTAINER, customDatabase.getDatabaseName());
                Statement stmt = conn.createStatement()) {
            stmt.execute("SELECT pg_logical_emit_message(false, 'test_prefix', 'message1')");
            stmt.execute("SELECT pg_logical_emit_message(false, 'other_prefix', 'message2')");
            stmt.execute("SELECT pg_logical_emit_message(false, 'test_prefix', 'message3')");
        }

        latch.await(10, TimeUnit.SECONDS);

        assertThat(results)
                .anySatisfy(
                        s -> {
                            assertThat(s).contains("\"op\":\"m\"");
                            assertThat(s).contains("\"prefix\":\"test_prefix\"");
                            assertThat(s).contains("\"content\":\"message1\"");
                        })
                .anySatisfy(
                        s -> {
                            assertThat(s).contains("\"op\":\"m\"");
                            assertThat(s).contains("\"prefix\":\"test_prefix\"");
                            assertThat(s).contains("\"content\":\"message3\"");
                        })
                .noneSatisfy(
                        s -> {
                            assertThat(s).contains("\"prefix\":\"other_prefix\"");
                        });

        producerThread.interrupt();
    }

    private static class LogicalMessageDeserializer
            implements DebeziumDeserializationSchema<String> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
            if (record.value() instanceof Struct) {
                Struct value = (Struct) record.value();
                if ("m".equals(value.getString(Envelope.FieldName.OPERATION))) {
                    ObjectNode node = objectMapper.createObjectNode();
                    node.put("op", "m");
                    node.put("prefix", value.getString("message_prefix"));
                    node.put("content", value.getString("message_content"));
                    out.collect(node.toString());
                }
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
