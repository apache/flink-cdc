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

package org.apache.flink.cdc.connectors.mysql.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** IT Tests for {@link MySqlSource}. */
@Timeout(value = 20, unit = TimeUnit.SECONDS)
class MySqlSourceSSLConnectionITCase extends MySqlSourceTestBase {

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    private final List<String> initialData =
            Arrays.asList(
                    "{\"id\":101,\"name\":\"scooter\",\"description\":\"Small 2-wheel scooter\",\"weight\":3.14}",
                    "{\"id\":102,\"name\":\"car battery\",\"description\":\"12V car battery\",\"weight\":8.1}",
                    "{\"id\":103,\"name\":\"12-pack drill bits\",\"description\":\"12-pack of drill bits with sizes ranging from #40 to #3\",\"weight\":0.8}",
                    "{\"id\":104,\"name\":\"hammer\",\"description\":\"12oz carpenter's hammer\",\"weight\":0.75}",
                    "{\"id\":105,\"name\":\"hammer\",\"description\":\"14oz carpenter's hammer\",\"weight\":0.875}",
                    "{\"id\":106,\"name\":\"hammer\",\"description\":\"16oz carpenter's hammer\",\"weight\":1.0}",
                    "{\"id\":107,\"name\":\"rocks\",\"description\":\"box of assorted rocks\",\"weight\":5.3}",
                    "{\"id\":108,\"name\":\"jacket\",\"description\":\"water resistent black wind breaker\",\"weight\":0.1}",
                    "{\"id\":109,\"name\":\"spare tire\",\"description\":\"24 inch spare tire\",\"weight\":22.2}");

    @Test
    void testSetupMysqlSourceWithSSL() throws Exception {
        inventoryDatabase.createAndInitialize();

        // Enable SSL requirement on the MySQL side, all future connections must use SSL
        inventoryDatabase.enableSSLForUser();

        Properties jdbcConfig = new Properties();
        jdbcConfig.setProperty("useSSL", "true");
        jdbcConfig.setProperty("requireSSL", "true");
        jdbcConfig.setProperty("verifyServerCertificate", "false");

        Properties debeziumConfig = new Properties();
        debeziumConfig.setProperty("database.ssl.mode", "required");
        debeziumConfig.setProperty("database.ssl.trustServerCertificate", "true");

        Instant startTime = Instant.now().minusSeconds(100);

        MySqlSource<String> mySqlSource =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .tableList(inventoryDatabase.getDatabaseName() + ".products")
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .jdbcProperties(jdbcConfig)
                        .debeziumProperties(debeziumConfig)
                        .serverId("5401-5404")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .serverTimeZone("UTC")
                        .includeSchemaChanges(true) // output the schema changes as well
                        .startupOptions(StartupOptions.timestamp(startTime.toEpochMilli()))
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        DataStreamSource<String> source =
                env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                        .setParallelism(4);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (CloseableIterator<String> iterator = source.executeAndCollect()) {
            List<String> rows = new ArrayList<>();
            int expectedSize = initialData.size();
            long timeoutSeconds = 30;

            while (rows.size() < expectedSize) {
                // Wrap the blocking hasNext() call in a CompletableFuture with timeout
                CompletableFuture<Boolean> hasNextFuture =
                        CompletableFuture.supplyAsync(iterator::hasNext, executor);

                try {
                    Boolean hasNext = hasNextFuture.get(timeoutSeconds, TimeUnit.SECONDS);
                    if (hasNext) {
                        String next = iterator.next();
                        rows.add(next);
                    } else {
                        // No more data available
                        break;
                    }
                } catch (java.util.concurrent.TimeoutException e) {
                    throw new TimeoutException(
                            ("Timeout while waiting for records, application"
                                    + " is likely unable to process data from MySQL over SSL"));
                } catch (ExecutionException e) {
                    throw new RuntimeException(
                            "Error while checking for next element", e.getCause());
                }
            }

            Assertions.assertThat(rows)
                    .withFailMessage("should read all initial records")
                    .hasSize(expectedSize);
        } finally {
            executor.shutdownNow();
            env.close();
        }
    }
}
