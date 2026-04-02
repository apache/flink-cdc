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

package org.apache.flink.cdc.connectors.db2.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Db2SourceConfigFactory}. */
class Db2SourceConfigFactoryTest {

    @Test
    void testFetchSizePropagatedToDebeziumProperties() {
        Db2SourceConfigFactory factory = createFactory();
        factory.fetchSize(5000);

        Db2SourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("query.fetch.size")).isEqualTo("5000");
        assertThat(config.getDbzConnectorConfig().getQueryFetchSize()).isEqualTo(5000);
    }

    @Test
    void testDefaultFetchSizePropagatedToDebeziumProperties() {
        Db2SourceConfigFactory factory = createFactory();

        Db2SourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("query.fetch.size")).isEqualTo("1024");
        assertThat(config.getDbzConnectorConfig().getQueryFetchSize()).isEqualTo(1024);
    }

    @Test
    void testDebeziumPropertiesCanOverrideFetchSize() {
        Db2SourceConfigFactory factory = createFactory();
        factory.fetchSize(5000);
        Properties dbzProps = new Properties();
        dbzProps.setProperty("query.fetch.size", "8000");
        factory.debeziumProperties(dbzProps);

        Db2SourceConfig config = factory.create(0);

        assertThat(config.getDbzProperties().getProperty("query.fetch.size")).isEqualTo("8000");
        assertThat(config.getDbzConnectorConfig().getQueryFetchSize()).isEqualTo(8000);
    }

    private static Db2SourceConfigFactory createFactory() {
        Db2SourceConfigFactory factory = new Db2SourceConfigFactory();
        factory.hostname("localhost");
        factory.port(50000);
        factory.databaseList("myDB");
        factory.username("user");
        factory.password("password");
        factory.startupOptions(StartupOptions.initial());
        return factory;
    }
}
