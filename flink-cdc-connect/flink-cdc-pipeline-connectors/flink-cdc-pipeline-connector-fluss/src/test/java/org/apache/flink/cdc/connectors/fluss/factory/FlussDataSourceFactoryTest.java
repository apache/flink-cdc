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

package org.apache.flink.cdc.connectors.fluss.factory;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.utils.FactoryDiscoveryUtils;
import org.apache.flink.cdc.connectors.fluss.source.FlussDataSource;
import org.apache.flink.cdc.connectors.fluss.source.FlussDataSourceOptions;
import org.apache.flink.table.api.ValidationException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlussDataSourceFactory}. */
class FlussDataSourceFactoryTest {

    @Test
    void testCreateDataSource() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(FlussDataSourceFactory.class);

        Configuration conf = createValidConfiguration();
        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(FlussDataSource.class);
    }

    @Test
    void testCreateDataSourceWithAllOptions() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(),
                                        "localhost:9123")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_TYPE.key(),
                                        "fluss-default")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "pattern",
                                        "test_db\\.orders_.*")
                                .put(FlussDataSourceOptions.SCAN_STARTUP_MODE.key(), "latest")
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(FlussDataSource.class);
    }

    @Test
    void testCreateDataSourceWithJdbcDiscoverer() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(),
                                        "localhost:9123")
                                .put(FlussDataSourceOptions.TABLE_DISCOVERER_TYPE.key(), "jdbc")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "jdbc-url",
                                        "jdbc:mysql://localhost:3306/meta_db")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "table-name",
                                        "subscription_list")
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(FlussDataSource.class);
    }

    @Test
    void testUnsupportedOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory).isInstanceOf(FlussDataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(),
                                        "localhost:9123")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "pattern",
                                        "test_db\\..*")
                                .put("unsupported_key", "unsupported_value")
                                .build());

        Assertions.assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'fluss'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    void testMissingRequiredOption() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);

        // Missing bootstrap.servers
        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "pattern",
                                        "test_db\\..*")
                                .build());

        assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("bootstrap.servers");
    }

    @Test
    void testUnsupportedDiscovererType() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(),
                                        "localhost:9123")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_TYPE.key(),
                                        "unknown-type")
                                .build());

        assertThatThrownBy(
                        () ->
                                sourceFactory.createDataSource(
                                        new FactoryHelper.DefaultContext(
                                                conf,
                                                conf,
                                                Thread.currentThread().getContextClassLoader())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Unsupported object id discoverer factory for type 'unknown-type'");
    }

    @Test
    void testPrefixClientProperties() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);

        Configuration conf =
                Configuration.fromMap(
                        ImmutableMap.<String, String>builder()
                                .put(
                                        FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(),
                                        "localhost:9123")
                                .put(
                                        FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX
                                                + "pattern",
                                        "test_db\\..*")
                                .put("properties.client.request.timeout.ms", "5000")
                                .put("properties.client.id", "my-client")
                                .build());

        DataSource dataSource =
                sourceFactory.createDataSource(
                        new FactoryHelper.DefaultContext(
                                conf, conf, Thread.currentThread().getContextClassLoader()));
        Assertions.assertThat(dataSource).isInstanceOf(FlussDataSource.class);
    }

    @Test
    void testIdentifier() {
        DataSourceFactory sourceFactory =
                FactoryDiscoveryUtils.getFactoryByIdentifier("fluss", DataSourceFactory.class);
        Assertions.assertThat(sourceFactory.identifier()).isEqualTo("fluss");
    }

    private Configuration createValidConfiguration() {
        return Configuration.fromMap(
                ImmutableMap.<String, String>builder()
                        .put(FlussDataSourceOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123")
                        .put(
                                FlussDataSourceOptions.TABLE_DISCOVERER_OPTIONS_PREFIX + "pattern",
                                "test_db\\..*")
                        .build());
    }
}
