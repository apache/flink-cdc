/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.postgres.source.fetch;

import com.ververica.cdc.connectors.postgres.testutils.TestHelper;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.spi.OffsetContext;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.debezium.connector.postgresql.Utils.lastKnownLsn;
import static org.junit.Assert.assertEquals;

/** Unit test for {@link PostgresSourceFetchTaskContext}. */
public class PostgresSourceFetchTaskContextTest {

    private PostgresConnectorConfig connectorConfig;
    private OffsetContext.Loader<PostgresOffsetContext> offsetLoader;

    @Before
    public void beforeEach() {
        this.connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig().build());
        this.offsetLoader = new PostgresOffsetContext.Loader(this.connectorConfig);
    }

    @Test
    public void shouldNotResetLsnWhenLastCommitLsnIsNull() {
        final Map<String, Object> offsetValues = new HashMap<>();
        offsetValues.put(SourceInfo.LSN_KEY, 12345L);
        offsetValues.put(SourceInfo.TIMESTAMP_USEC_KEY, 67890L);
        offsetValues.put(PostgresOffsetContext.LAST_COMMIT_LSN_KEY, null);

        final PostgresOffsetContext offsetContext = offsetLoader.load(offsetValues);
        assertEquals(lastKnownLsn(offsetContext), Lsn.valueOf(12345L));
    }
}
