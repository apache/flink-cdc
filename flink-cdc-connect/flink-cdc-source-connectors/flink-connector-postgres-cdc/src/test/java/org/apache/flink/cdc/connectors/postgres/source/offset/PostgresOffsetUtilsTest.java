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

package org.apache.flink.cdc.connectors.postgres.source.offset;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresOffsetUtils}. */
class PostgresOffsetUtilsTest {

    /**
     * Debezium 2.1 added {@code messageType} to the Postgres offset. Its value is an operation name
     * such as {@code INSERT}, so the offset map is no longer "all longs" and must not be parsed as
     * such - otherwise every stream split fails with {@code NumberFormatException}.
     */
    @Test
    void testOffsetWithNonNumericValuesIsRestored() {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SourceInfo.LSN_KEY, "23456789");
        offsetMap.put(SourceInfo.TXID_KEY, "755");
        offsetMap.put(SourceInfo.TIMESTAMP_USEC_KEY, "1690000000000000");
        offsetMap.put(SourceInfo.MSG_TYPE_KEY, "INSERT");
        offsetMap.put("snapshot", "false");
        offsetMap.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, "false");

        PostgresOffsetContext offsetContext =
                PostgresOffsetUtils.getPostgresOffsetContext(
                        new PostgresOffsetContext.Loader(
                                new PostgresConnectorConfig(
                                        io.debezium.config.Configuration.create()
                                                .with("topic.prefix", "test_server")
                                                .with("database.hostname", "localhost")
                                                .with("database.user", "postgres")
                                                .with("database.password", "postgres")
                                                .with("database.dbname", "postgres")
                                                .with("plugin.name", "pgoutput")
                                                .with("slot.name", "flink")
                                                .build())),
                        new PostgresOffset(offsetMap));

        Map<String, ?> restored = offsetContext.getOffset();
        assertThat(restored.get(SourceInfo.LSN_KEY)).isEqualTo(Lsn.valueOf(23456789L).asLong());
        assertThat(restored.get(SourceInfo.TXID_KEY)).isEqualTo(755L);
    }
}
