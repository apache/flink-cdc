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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Test for {@link BinlogOffsetSerializer} state-recovery compatibility: a checkpoint written
 * by the Pre-MariaDB-GTID-support version, must still deserialize losslessly with the current code,
 * and the recovered MySQL GTID offset must route and order identically. The MariaDB-GTID-support PR
 * add no field to {@link BinlogOffset} and left the serializer untouched, so existing MySQL Jobs
 * restore from old savepoint with zero migration.
 */
class BinlogOffsetSerializerTest {

    //
    private static final String LEGACY_MYSQL_GTID_JSON =
            "{\n"
                    + "  \"file\" : \"mysql-bin.000003\",\n"
                    + "  \"pos\" : \"4567\",\n"
                    + "  \"event\" : \"2\",\n"
                    + "  \"row\" : \"1\",\n"
                    + "  \"gtids\" : \"24bc7850-2c16-11e6-a073-0242ac110002:1-1116\",\n"
                    + "  \"server_id\" : \"223344\",\n"
                    + "  \"ts_sec\": \"170000000\"\n"
                    + "}";
    private static final String MYSQL_UUID = "24bc7850-2c16-11e6-a073-0242ac110002";

    @Test
    void deserializesLegacyGtidOffset() throws Exception {
        BinlogOffset offset =
                BinlogOffsetSerializer.INSTANCE.deserialize(
                        LEGACY_MYSQL_GTID_JSON.getBytes(StandardCharsets.UTF_8));

        assertThat(offset.getFilename()).isEqualTo("mysql-bin.000003");
        assertThat(offset.getPosition()).isEqualTo(4567L);
        assertThat(offset.getRestartSkipEvents()).isEqualTo(2L);
        assertThat(offset.getRestartSkipRows()).isEqualTo(1);
        assertThat(offset.getGtidSet()).isEqualTo(MYSQL_UUID + ":1-1116");
        assertThat(offset.getServerId()).isEqualTo(223344L);
        assertThat(offset.getTimestampSec()).isEqualTo(170000000L);

        // The offset stays stateless after restore: no dialect marker was ever stored,
        // and the refactor did not add one
        assertThat(offset.getOffset()).doesNotContainKey("dialect");
    }

    @Test
    void roundTripsThroughCurrentSerializer() throws Exception {
        BinlogOffsetSerializer serializer = BinlogOffsetSerializer.INSTANCE;

        BinlogOffset original =
                serializer.deserialize(LEGACY_MYSQL_GTID_JSON.getBytes(StandardCharsets.UTF_8));

        byte[] reSerialized = serializer.serialize(original);
        BinlogOffset restoredOffset = serializer.deserialize(reSerialized);

        assertThat(restoredOffset).isEqualTo(original);
        assertThat(restoredOffset.getOffset()).isEqualTo(original.getOffset());
    }

    @Test
    void recoveredMysqlGtidOffsetRoutesToMysqlStrategy() throws Exception {
        BinlogOffsetSerializer serializer = BinlogOffsetSerializer.INSTANCE;
        BinlogOffset offset =
                serializer.deserialize(LEGACY_MYSQL_GTID_JSON.getBytes(StandardCharsets.UTF_8));

        assertThat(GtidStrategies.detect(offset.getGtidSet()))
                .isInstanceOf(MysqlGtidStrategy.class);
    }

    @Test
    void compareToOrderingUnchangedForRecoveredMysqlGtids() {
        // Drive the actual recovery comparison path (BinlogOffset.compareTo ->
        // GtidStrategies.detect -> MySqlGtidStrategy) end to end, not just the strategy in
        // isolation, to prove offset seeking keeps the pre-refactor ordering for MySQL GTIDs.
        BinlogOffset earlier = BinlogOffset.ofGtidSet(MYSQL_UUID + ":1-1116");
        BinlogOffset later = BinlogOffset.ofGtidSet(MYSQL_UUID + ":1-2000");

        assertThat(earlier.isBefore(later)).isTrue();
        assertThat(later.isAfter(earlier)).isTrue();
        assertThat(earlier.isAtOrBefore(earlier)).isTrue();
        assertThat(earlier.compareTo(earlier)).isZero();
    }
}
