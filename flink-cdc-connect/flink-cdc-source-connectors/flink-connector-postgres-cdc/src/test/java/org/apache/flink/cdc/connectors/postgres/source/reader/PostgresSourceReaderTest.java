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

import org.apache.flink.cdc.connectors.postgres.source.MockPostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import org.apache.flink.cdc.connectors.postgres.testutils.TestTable;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSourceReader}. */
class PostgresSourceReaderTest {

    @Test
    void testNotifyCheckpointWindowSizeOne() throws Exception {
        final PostgresSourceReader reader = createReader(1);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(11L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(12L);
        assertThat(completedCheckpointIds).containsExactly(11L);
        reader.notifyCheckpointComplete(13L);
        assertThat(completedCheckpointIds).containsExactly(11L, 12L);
    }

    @Test
    void testNotifyCheckpointWindowSizeDefault() throws Exception {
        final PostgresSourceReader reader = createReader(3);
        final List<Long> completedCheckpointIds = new ArrayList<>();
        MockPostgresDialect.setNotifyCheckpointCompleteCallback(
                id -> completedCheckpointIds.add(id));
        reader.notifyCheckpointComplete(103L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(102L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(101L);
        assertThat(completedCheckpointIds).isEmpty();
        reader.notifyCheckpointComplete(104L);
        assertThat(completedCheckpointIds).containsExactly(101L);
    }

    private PostgresSourceReader createReader(final int lsnCommitCheckpointsDelay)
            throws Exception {
        final PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        final PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname("host");
        configFactory.database("pgdb");
        configFactory.username("username");
        configFactory.password("password");
        configFactory.setLsnCommitCheckpointsDelay(lsnCommitCheckpointsDelay);
        final TestTable customerTable =
                new TestTable(
                        "pgdb",
                        "customer",
                        "customers",
                        ResolvedSchema.of(Column.physical("id", BIGINT())));
        final DebeziumDeserializationSchema<?> deserializer = customerTable.getDeserializer();
        MockPostgresDialect dialect = new MockPostgresDialect(configFactory.create(0));
        final PostgresSourceBuilder.PostgresIncrementalSource<?> source =
                new PostgresSourceBuilder.PostgresIncrementalSource<>(
                        configFactory, checkNotNull(deserializer), offsetFactory, dialect);
        return source.createReader(new TestingReaderContext());
    }
}
