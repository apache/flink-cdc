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

package org.apache.flink.cdc.connectors.jdbc;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.connectors.jdbc.common.JdbcSinkTestBase;
import org.apache.flink.cdc.connectors.jdbc.config.JdbcSinkConfig;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialect;
import org.apache.flink.cdc.connectors.jdbc.dialect.JdbcSinkDialectFactory;
import org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Integrated test cases for generic Jdbc sink connector. */
class JdbcSinkGenericITCase extends JdbcSinkTestBase {

    @Test
    void testRunJdbcWithUnknownDialect() {
        Configuration sinkConfig = new Configuration();

        sinkConfig.set(JdbcSinkOptions.CONN_URL, "jdbc:unexpected://localhost:3306");
        sinkConfig.set(JdbcSinkOptions.USERNAME, "username");
        sinkConfig.set(JdbcSinkOptions.PASSWORD, "password");

        Assertions.assertThatThrownBy(() -> runJobWithEvents(sinkConfig, Collections.emptyList()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "There must be exactly one factory for dialect unexpected, but 0 found.\n"
                                + "Matched dialects: []\n"
                                + "Available dialects: [dummy, dummy, mysql]");
    }

    @Test
    void testRunJdbcWithDuplicatedDialect() throws Exception {
        Configuration sinkConfig = new Configuration();

        sinkConfig.set(JdbcSinkOptions.CONN_URL, "jdbc:dummy://localhost:3306");
        sinkConfig.set(JdbcSinkOptions.USERNAME, "username");
        sinkConfig.set(JdbcSinkOptions.PASSWORD, "password");

        Assertions.assertThatThrownBy(() -> runJobWithEvents(sinkConfig, Collections.emptyList()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "There must be exactly one factory for dialect dummy, but 2 found.\n"
                                + "Matched dialects: [class org.apache.flink.cdc.connectors.jdbc.JdbcSinkGenericITCase$DummyFactoryA, class org.apache.flink.cdc.connectors.jdbc.JdbcSinkGenericITCase$DummyFactoryB]\n"
                                + "Available dialects: [dummy, dummy, mysql]");
    }

    public static class DummyFactoryA implements JdbcSinkDialectFactory<JdbcSinkConfig> {

        @Override
        public String identifier() {
            return "dummy";
        }

        @Override
        public JdbcSinkDialect createDialect(JdbcSinkConfig option) {
            throw new UnsupportedOperationException(
                    "This is a dummy dialect factory and is not meant to be instantiated.");
        }
    }

    public static class DummyFactoryB implements JdbcSinkDialectFactory<JdbcSinkConfig> {
        @Override
        public String identifier() {
            return "dummy";
        }

        @Override
        public JdbcSinkDialect createDialect(JdbcSinkConfig option) {
            throw new UnsupportedOperationException(
                    "This is a dummy dialect factory and is not meant to be instantiated.");
        }
    }
}
