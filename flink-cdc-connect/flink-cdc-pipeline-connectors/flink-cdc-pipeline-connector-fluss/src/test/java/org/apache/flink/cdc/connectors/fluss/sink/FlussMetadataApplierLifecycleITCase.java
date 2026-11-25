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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DropTableEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.util.InstantiationUtil;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.config.ConfigOptions.NETTY_CLIENT_NUM_NETWORK_THREADS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Tests metadata client ownership with a real Fluss cluster. */
class FlussMetadataApplierLifecycleITCase {
    @RegisterExtension
    private static final FlussClusterExtension CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    @Test
    void reusesClientThreadsAcrossSchemaOperationsAndClosesThem() throws Exception {
        TableId tableId = TableId.tableId("lifecycle", "reuse");
        Schema schema = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        Set<Thread> clientThreads;
        try (Connection observer = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = observer.getAdmin();
                FlussMetaDataApplier applier = newApplier()) {
            admin.createDatabase("lifecycle", DatabaseDescriptor.EMPTY, true).get();
            admin.tableExists(new TablePath("lifecycle", "reuse")).get();
            Set<Thread> before = clientThreads();
            assertThat(applier.getExistingTableSchema(tableId)).isEmpty();
            clientThreads = newClientThreads(before);
            assertThat(clientThreads).hasSize(1);

            for (int i = 0; i < 3; i++) {
                TableId nextTable = TableId.tableId("lifecycle", "reuse_" + i);
                applier.applySchemaChange(new CreateTableEvent(nextTable, schema));
                assertThat(applier.getExistingTableSchema(nextTable)).contains(schema);
                applier.applySchemaChange(
                        new AddColumnEvent(
                                nextTable,
                                Collections.singletonList(
                                        new AddColumnEvent.ColumnWithPosition(
                                                Column.physicalColumn("extra", DataTypes.INT())))));
                assertThat(applier.getExistingTableSchema(nextTable).get().getColumnNames())
                        .containsExactly("id", "extra");
                applier.applySchemaChange(new DropTableEvent(nextTable));
                assertThat(applier.getExistingTableSchema(nextTable)).isEmpty();
                assertThat(newClientThreads(before))
                        .containsExactlyInAnyOrderElementsOf(clientThreads);
            }
            applier.close();
            assertThreadsStopped(clientThreads);
            applier.close();
        }
        assertThreadsStopped(clientThreads);
    }

    @Test
    void ownsIndependentClientsAfterSerializationAndReopening() throws Exception {
        TableId tableId = TableId.tableId("lifecycle", "missing");
        try (Connection observer = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = observer.getAdmin();
                FlussMetaDataApplier original = newApplier();
                FlussMetaDataApplier independent = newApplier()) {
            admin.tableExists(new TablePath("lifecycle", "missing")).get();
            Set<Thread> before = clientThreads();
            original.getExistingTableSchema(tableId);
            Set<Thread> originalThreads = newClientThreads(before);
            assertThat(originalThreads).hasSize(1);
            before = clientThreads();
            independent.getExistingTableSchema(tableId);
            Set<Thread> independentThreads = newClientThreads(before);
            assertThat(independentThreads).hasSize(1);
            Set<Thread> restoredThreads;
            try (FlussMetaDataApplier restored = InstantiationUtil.clone(original)) {
                before = clientThreads();
                assertThat(restored.getExistingTableSchema(tableId)).isEmpty();
                restoredThreads = newClientThreads(before);
                assertThat(restoredThreads).hasSize(1);
            }
            assertThreadsStopped(restoredThreads);
            assertThat(clientThreads())
                    .containsAll(originalThreads)
                    .containsAll(independentThreads);
            original.close();
            assertThreadsStopped(originalThreads);
            assertThat(clientThreads()).containsAll(independentThreads);
            before = clientThreads();
            assertThat(original.getExistingTableSchema(tableId)).isEmpty();
            Set<Thread> reopenedThreads = newClientThreads(before);
            assertThat(reopenedThreads).hasSize(1);
            original.close();
            independent.close();
            assertThreadsStopped(reopenedThreads);
            assertThreadsStopped(independentThreads);
        }
    }

    @Test
    void closesClientAfterSchemaFailure() throws Exception {
        TableId tableId = TableId.tableId("lifecycle", "failure");
        Schema initial = Schema.newBuilder().physicalColumn("id", DataTypes.INT()).build();
        Schema conflicting =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        Set<Thread> ownedThreads;
        try (Connection observer = ConnectionFactory.createConnection(CLUSTER.getClientConfig());
                Admin admin = observer.getAdmin();
                FlussMetaDataApplier applier = newApplier()) {
            admin.createDatabase("lifecycle", DatabaseDescriptor.EMPTY, true).get();
            admin.tableExists(new TablePath("lifecycle", "failure")).get();
            Set<Thread> before = clientThreads();
            applier.getExistingTableSchema(tableId);
            ownedThreads = newClientThreads(before);
            assertThat(ownedThreads).hasSize(1);
            applier.applySchemaChange(new CreateTableEvent(tableId, initial));
            assertThatThrownBy(
                            () ->
                                    applier.applySchemaChange(
                                            new CreateTableEvent(tableId, conflicting)))
                    .hasMessageContaining("primary keys");
            assertThat(applier.getExistingTableSchema(tableId)).contains(initial);
        }
        assertThreadsStopped(ownedThreads);
    }

    private static FlussMetaDataApplier newApplier() {
        Configuration config = Configuration.fromMap(CLUSTER.getClientConfig().toMap());
        config.set(NETTY_CLIENT_NUM_NETWORK_THREADS, 1);
        return new FlussMetaDataApplier(
                config, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    private static Set<Thread> clientThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(thread -> thread.getName().startsWith("fluss-netty-client"))
                .collect(Collectors.toSet());
    }

    private static Set<Thread> newClientThreads(Set<Thread> before) {
        Set<Thread> threads = new HashSet<>(clientThreads());
        threads.removeAll(before);
        return threads;
    }

    private static void assertThreadsStopped(Set<Thread> threads) {
        await().untilAsserted(() -> assertThat(threads).noneMatch(Thread::isAlive));
    }
}
