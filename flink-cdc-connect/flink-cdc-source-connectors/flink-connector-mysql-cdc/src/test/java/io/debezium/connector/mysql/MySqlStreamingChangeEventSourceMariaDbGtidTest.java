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
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer.EventDataWrapper;
import org.junit.jupiter.api.Test;

import static io.debezium.connector.mysql.MySqlStreamingChangeEventSource.mariadbGtidOf;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit Test for the MariaDB GTID resume/ingest {@link MySqlStreamingChangeEventSource} */
class MySqlStreamingChangeEventSourceMariaDbGtidTest {

    /**
     * The GTID must use the server id from the event header, not the payload. The shyiko 0.27.2
     * {@code MariadbGtidEventDataDeserializer} never sets the payload server id (always 0), so
     * reading it would ckp "0-0-N" -dirty state that survives restarts.
     */
    @Test
    void mariadbGtidOfUsesHeaderServerIdNotBrokenPayloadServerId() {
        EventHeaderV4 eventHeaderV4 = new EventHeaderV4();
        eventHeaderV4.setEventType(EventType.MARIADB_GTID);
        eventHeaderV4.setServerId(7L);

        MariadbGtidEventData data = new MariadbGtidEventData();
        data.setDomainId(0);
        data.setSequence(42L);

        // Reproduce the deserializer bug: the payload server id is never populated
        assertThat(data.getServerId()).isEqualTo(0L);

        assertThat(mariadbGtidOf(new Event(eventHeaderV4, data))).isEqualTo("0-7-42");
    }

    @Test
    void mariadbGtidOfUnwrapsEventDataWrapper() {
        EventHeaderV4 eventHeaderV4 = new EventHeaderV4();
        eventHeaderV4.setEventType(EventType.MARIADB_GTID);
        eventHeaderV4.setServerId(5L);

        MariadbGtidEventData data = new MariadbGtidEventData();
        data.setDomainId(1L);
        data.setSequence(99L);

        Event event = new Event(eventHeaderV4, new EventDataWrapper(data, data));
        assertThat(mariadbGtidOf(event)).isEqualTo("1-5-99");
    }
}
