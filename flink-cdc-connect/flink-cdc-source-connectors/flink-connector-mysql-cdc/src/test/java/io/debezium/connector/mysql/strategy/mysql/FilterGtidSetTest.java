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

package io.debezium.connector.mysql.strategy.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.GtidSet;
import io.debezium.connector.mysql.strategy.ConnectionConfiguration;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Integration test for {@link MySqlConnection#filterGtidSet} to ensure the LATEST mode fix
 * (FLINK-39149) cannot regress.
 *
 * <p>Debezium 2.5 moved the GTID merge from the streaming source onto the connection, so the test
 * drives {@link MySqlConnection} rather than {@code MySqlStreamingChangeEventSource}.
 */
class FilterGtidSetTest {

    @Test
    void testFilterGtidSetLatestModeFixesNonContiguousGtid() throws Exception {
        MySqlConnection connection = createConnectionWithConfig("latest");
        GtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
        GtidSet purgedServerGtid = new MySqlGtidSet("");

        GtidSet result =
                connection.filterGtidSet(
                        null,
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000",
                        availableServerGtidSet,
                        purgedServerGtid);

        assertThat(result.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110001:1-8000");
        assertThat(result.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
    }

    @Test
    void testFilterGtidSetLatestModeWithSourceFilter() throws Exception {
        Predicate<String> excludeCcc = uuid -> !uuid.equals("24bc7850-2c16-11e6-a073-0242ac110003");
        MySqlConnection connection = createConnectionWithConfig("latest");
        GtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000,24bc7850-2c16-11e6-a073-0242ac110003:1-5000");
        GtidSet purgedServerGtid = new MySqlGtidSet("");

        GtidSet result =
                connection.filterGtidSet(
                        excludeCcc,
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000",
                        availableServerGtidSet,
                        purgedServerGtid);

        assertThat(result.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110001:1-8000");
        assertThat(result.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110002:1-2000");
        assertThat(result.toString()).doesNotContain("24bc7850-2c16-11e6-a073-0242ac110003");
    }

    @Test
    void testFilterGtidSetEarliestModeNotAffected() throws Exception {
        MySqlConnection connection = createConnectionWithConfig("earliest");
        GtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
        GtidSet purgedServerGtid = new MySqlGtidSet("");

        GtidSet result =
                connection.filterGtidSet(
                        null,
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000",
                        availableServerGtidSet,
                        purgedServerGtid);

        assertThat(result.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110001:1-8000");
        assertThat(((MySqlGtidSet) result).forServerWithId("24bc7850-2c16-11e6-a073-0242ac110002"))
                .isNull();
    }

    @Test
    void testFilterGtidSetReturnsNullWhenNoGtid() throws Exception {
        MySqlConnection connection = createConnectionWithConfig("latest");
        GtidSet availableServerGtidSet =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110001:1-10000");
        GtidSet purgedServerGtid = new MySqlGtidSet("");

        GtidSet result =
                connection.filterGtidSet(null, null, availableServerGtidSet, purgedServerGtid);

        assertThat(result).isNull();
    }

    private static MySqlConnection createConnectionWithConfig(String channelPosition)
            throws Exception {
        Configuration dbzConfig =
                Configuration.create().with("gtid.new.channel.position", channelPosition).build();

        ConnectionConfiguration mockConnectionConfig = Mockito.mock(ConnectionConfiguration.class);
        when(mockConnectionConfig.originalConfig()).thenReturn(dbzConfig);

        MySqlConnection connection =
                Mockito.mock(MySqlConnection.class, Mockito.CALLS_REAL_METHODS);

        Field configField =
                Class.forName("io.debezium.connector.mysql.strategy.AbstractConnectorConnection")
                        .getDeclaredField("connectionConfig");
        configField.setAccessible(true);
        configField.set(connection, mockConnectionConfig);

        return connection;
    }
}
