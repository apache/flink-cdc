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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.mongodb.factory.MongoDBDataSourceFactory;
import org.apache.flink.table.api.ValidationException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.HOSTS;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.PASSWORD;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.TABLES;
import static org.apache.flink.cdc.connectors.mongodb.source.MongoDBDataSourceOptions.USERNAME;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link MongoDBDataSourceFactory}. */
@RunWith(Parameterized.class)
public class MongoDBDataSourceFactoryTest extends MongoDBSourceTestBase {

    public MongoDBDataSourceFactoryTest(String mongoVersion) {
        super(mongoVersion);
    }

    @Parameterized.Parameters(name = "mongoVersion: {0}")
    public static Object[] parameters() {
        return new Object[][] {new Object[] {"6.0.16"}, new Object[] {"7.0.12"}};
    }

    @Test
    public void testCreateSource() {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");
        Map<String, String> options = new HashMap<>();
        options.put(HOSTS.key(), mongoContainer.getHostAndPort());
        options.put(USERNAME.key(), FLINK_USER);
        options.put(PASSWORD.key(), FLINK_USER_PASSWORD);
        options.put(TABLES.key(), database + ".prod\\.*");
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MongoDBDataSourceFactory factory = new MongoDBDataSourceFactory();
        MongoDBDataSource dataSource = (MongoDBDataSource) factory.createDataSource(context);
        assertThat(dataSource.getSourceConfig().getCollectionList())
                .isEqualTo(Arrays.asList(database + ".products"));
    }

    @Test
    public void testNoMatchedTable() {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");
        Map<String, String> options = new HashMap<>();
        options.put(HOSTS.key(), mongoContainer.getHostAndPort());
        options.put(USERNAME.key(), FLINK_USER);
        options.put(PASSWORD.key(), FLINK_USER_PASSWORD);
        options.put(TABLES.key(), database + ".test");
        String tables = database + ".test";
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        MongoDBDataSourceFactory factory = new MongoDBDataSourceFactory();
        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Cannot find any collection by the option 'tables' = " + tables);
    }

    @Test
    public void testLackRequireOption() {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");
        Map<String, String> options = new HashMap<>();
        options.put(HOSTS.key(), mongoContainer.getHostAndPort());
        options.put(USERNAME.key(), FLINK_USER);
        options.put(PASSWORD.key(), FLINK_USER_PASSWORD);
        options.put(TABLES.key(), database + ".prod\\.*");

        MongoDBDataSourceFactory factory = new MongoDBDataSourceFactory();
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    public void testUnsupportedOption() {
        String database = mongoContainer.executeCommandFileInSeparateDatabase("inventory");
        Map<String, String> options = new HashMap<>();
        options.put(HOSTS.key(), mongoContainer.getHostAndPort());
        options.put(USERNAME.key(), FLINK_USER);
        options.put(PASSWORD.key(), FLINK_USER_PASSWORD);
        options.put(TABLES.key(), database + ".prod\\.*");
        options.put("unsupported_key", "unsupported_value");

        MongoDBDataSourceFactory factory = new MongoDBDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'mongodb'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
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
