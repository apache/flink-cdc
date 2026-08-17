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

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.postgres.source.PostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresEventDeserializer;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link PostgresPipelineRecordEmitter}. */
class PostgresPipelineRecordEmitterTest {

    private static final TableId DBZ_TABLE_ID = new TableId(null, "public", "users");

    @Test
    void testInitializationDoesNotLoadTableSchemas() {
        PostgresSourceConfig sourceConfig = createSourceConfig(StartupOptions.initial());

        assertThatCode(() -> createEmitter(sourceConfig)).doesNotThrowAnyException();
    }

    @Test
    void testSnapshotSplitAssignmentDoesNotLoadAllTableSchemas() {
        PostgresSourceConfig sourceConfig = createSourceConfig(StartupOptions.snapshot());
        PostgresPipelineRecordEmitter<Event> emitter = createEmitter(sourceConfig);

        assertThatCode(() -> emitter.applySplit(createSnapshotSplit())).doesNotThrowAnyException();
    }

    private static PostgresPipelineRecordEmitter<Event> createEmitter(
            PostgresSourceConfig sourceConfig) {
        return new PostgresPipelineRecordEmitter<>(
                new PostgresEventDeserializer(DebeziumChangelogMode.ALL),
                null,
                sourceConfig,
                new PostgresOffsetFactory(),
                new FailingPostgresDialect(sourceConfig));
    }

    private static PostgresSourceConfig createSourceConfig(StartupOptions startupOptions) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("localhost");
        configFactory.port(5432);
        configFactory.username("user");
        configFactory.password("password");
        configFactory.database("db0");
        configFactory.tableList("public.users");
        configFactory.startupOptions(startupOptions);
        return configFactory.create(0);
    }

    private static SnapshotSplit createSnapshotSplit() {
        return new SnapshotSplit(
                DBZ_TABLE_ID,
                "public.users:0",
                RowType.of(new IntType()),
                null,
                null,
                null,
                Collections.emptyMap());
    }

    private static class FailingPostgresDialect extends PostgresDialect {
        private FailingPostgresDialect(PostgresSourceConfig sourceConfig) {
            super(sourceConfig);
        }

        @Override
        public PostgresConnection openJdbcConnection() {
            throw new AssertionError("JDBC connection should be opened lazily.");
        }
    }
}
