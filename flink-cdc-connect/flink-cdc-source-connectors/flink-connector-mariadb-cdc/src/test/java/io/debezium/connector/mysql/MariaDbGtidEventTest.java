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

package io.debezium.connector.mysql;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for {@link MySqlStreamingChangeEventSource#mariadbGtidOf(Event)}, the MariaDB GTID
 * formatting used to advance the consumed GTID position while streaming binlog events.
 *
 * <p>Lives in {@code io.debezium.connector.mysql} because {@code mariadbGtidOf} is package-private
 * on the shadowed {@code MySqlStreamingChangeEventSource}.
 */
class MariaDbGtidEventTest {

    /**
     * The formatted GTID must be {@code domain-serverId-sequence} where the server id is taken from
     * the binlog event header, not the payload. MariaDB records GTIDs against the header server id
     * and exposes them through {@code @@gtid_binlog_pos}; using the payload server id would produce
     * a GTID that does not line up with the server's own set and would break GTID-based resume.
     */
    @Test
    void mariadbGtidUsesHeaderServerIdNotPayloadServerId() {
        MariadbGtidEventData data = new MariadbGtidEventData();
        data.setDomainId(1);
        // payload server id: deliberately different from the header
        data.setServerId(99);
        data.setSequence(42);

        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.MARIADB_GTID);
        // header server id: this is the one that must win
        header.setServerId(7);

        Event event = new Event(header, data);

        assertThat(MySqlStreamingChangeEventSource.mariadbGtidOf(event)).isEqualTo("1-7-42");
    }

    /** A GTID from a different domain formats independently of any other domain. */
    @Test
    void mariadbGtidFormatsDomainSequenceFromPayload() {
        MariadbGtidEventData data = new MariadbGtidEventData();
        data.setDomainId(5);
        data.setServerId(7);
        data.setSequence(1000);

        EventHeaderV4 header = new EventHeaderV4();
        header.setEventType(EventType.MARIADB_GTID);
        header.setServerId(7);

        Event event = new Event(header, data);

        assertThat(MySqlStreamingChangeEventSource.mariadbGtidOf(event)).isEqualTo("5-7-1000");
    }
}
