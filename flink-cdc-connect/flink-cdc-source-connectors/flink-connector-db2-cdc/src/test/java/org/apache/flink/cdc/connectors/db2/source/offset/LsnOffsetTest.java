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

package org.apache.flink.cdc.connectors.db2.source.offset;

import io.debezium.config.Configuration;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.connector.db2.Lsn;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that Debezium's Db2 offset loader restores an {@link LsnOffset} from Flink CDC's state. */
class LsnOffsetTest {

    private static final Db2ConnectorConfig CONFIG =
            new Db2ConnectorConfig(
                    Configuration.create()
                            .with("database.server.name", "db2")
                            .with("database.dbname", "cdc")
                            .build());

    private static final Lsn CHANGE_LSN = Lsn.valueOf(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 2});
    private static final Lsn COMMIT_LSN = Lsn.valueOf(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 1});

    @Test
    void testLoadOffsetWithEventSerialNo() {
        LsnOffset offset = new LsnOffset(CHANGE_LSN, COMMIT_LSN, 3L);

        Db2OffsetContext context = new Db2OffsetContext.Loader(CONFIG).load(offset.getOffset());

        assertThat(context.getEventSerialNo()).isEqualTo(3L);
        assertThat(context.getChangePosition().getInTxLsn()).isEqualTo(CHANGE_LSN);
        assertThat(context.getChangePosition().getCommitLsn()).isEqualTo(COMMIT_LSN);
    }

    @Test
    void testLoadOffsetWithoutEventSerialNo() {
        LsnOffset offset = new LsnOffset(CHANGE_LSN, COMMIT_LSN, null);

        Db2OffsetContext context = new Db2OffsetContext.Loader(CONFIG).load(offset.getOffset());

        assertThat(context.getEventSerialNo()).isZero();
        assertThat(context.getChangePosition().getInTxLsn()).isEqualTo(CHANGE_LSN);
    }
}
