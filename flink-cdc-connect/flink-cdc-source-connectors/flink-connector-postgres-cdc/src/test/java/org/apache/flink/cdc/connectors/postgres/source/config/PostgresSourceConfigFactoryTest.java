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

package org.apache.flink.cdc.connectors.postgres.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PostgresSourceConfigFactory}. */
class PostgresSourceConfigFactoryTest {

    @Test
    void testFetchSizePropagatedToDebeziumProperties() {
        PostgresSourceConfigFactory factory = createFactory();
        factory.fetchSize(5000);

        PostgresSourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("snapshot.fetch.size")).isEqualTo("5000");
        assertThat(config.getDbzConnectorConfig().getSnapshotFetchSize()).isEqualTo(5000);
    }

    @Test
    void testDefaultFetchSizePropagatedToDebeziumProperties() {
        PostgresSourceConfigFactory factory = createFactory();

        PostgresSourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("snapshot.fetch.size")).isEqualTo("1024");
        assertThat(config.getDbzConnectorConfig().getSnapshotFetchSize()).isEqualTo(1024);
    }

    @Test
    void testDebeziumPropertiesCanOverrideFetchSize() {
        PostgresSourceConfigFactory factory = createFactory();
        factory.fetchSize(5000);
        Properties dbzProps = new Properties();
        dbzProps.setProperty("snapshot.fetch.size", "8000");
        factory.debeziumProperties(dbzProps);

        PostgresSourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("snapshot.fetch.size")).isEqualTo("8000");
        assertThat(config.getDbzConnectorConfig().getSnapshotFetchSize()).isEqualTo(8000);
    }

    private static PostgresSourceConfigFactory createFactory() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("localhost");
        factory.port(5432);
        factory.database("myDB");
        factory.username("user");
        factory.password("password");
        factory.startupOptions(StartupOptions.initial());
        return factory;
    }
}
