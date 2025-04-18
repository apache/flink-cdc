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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.oracle.factory.OracleDataSourceFactory;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.lifecycle.Startables;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.DATABASE;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.HOSTNAME;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.PORT;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.oracle.source.OracleDataSourceOptions.USERNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link OracleDataSourceFactory}. */
public class OracleDataSourceFactoryTest extends OracleSourceTestBase {

    @BeforeAll
    public static void beforeClass() {
        LOG.info("Starting oracle19c containers...");
        Startables.deepStart(Stream.of(ORACLE_CONTAINER)).join();
        LOG.info("Container oracle19c is started.");
    }

    @AfterAll
    public static void afterClass() {
        LOG.info("Stopping oracle19c containers...");
        ORACLE_CONTAINER.stop();
        LOG.info("Container oracle19c is stopped.");
    }

    @Test
    public void testCreateSource() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(TABLES.key(), "debezium.products,debezium.category");
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        OracleDataSourceFactory factory = new OracleDataSourceFactory();
        OracleDataSource dataSource = (OracleDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getTableList())
                .isEqualTo(Arrays.asList("debezium.products", "debezium.category"));
    }

    @Test
    public void testNoMatchedTable() {
        Map<String, String> options = new HashMap<>();
        options.put(HOSTNAME.key(), ORACLE_CONTAINER.getHost());
        options.put(PORT.key(), String.valueOf(ORACLE_CONTAINER.getOraclePort()));
        options.put(USERNAME.key(), CONNECTOR_USER);
        options.put(PASSWORD.key(), CONNECTOR_PWD);
        options.put(DATABASE.key(), ORACLE_CONTAINER.getDatabaseName());
        String tables = "DEBEZIUM.TEST";
        options.put(TABLES.key(), tables);
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        OracleDataSourceFactory factory = new OracleDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot find any table by the option 'tables' = " + tables);
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
