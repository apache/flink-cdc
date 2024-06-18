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
import org.apache.flink.cdc.connectors.jdbc.options.JdbcSinkOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/** Integrated test cases for generic Jdbc sink connector. */
class JdbcSinkGenericITCase extends JdbcSinkTestBase {

    @Test
    void testRunJdbcWithUnknownDialect() throws Exception {
        Configuration sinkConfig = new Configuration();
        sinkConfig.set(JdbcSinkOptions.DIALECT, "unexpected");
        sinkConfig.set(JdbcSinkOptions.HOSTNAME, "localhost");
        sinkConfig.set(JdbcSinkOptions.PORT, 3306);
        sinkConfig.set(JdbcSinkOptions.USERNAME, "username");
        sinkConfig.set(JdbcSinkOptions.PASSWORD, "password");
        sinkConfig.set(JdbcSinkOptions.SERVER_TIME_ZONE, "UTC");

        Assertions.assertThatThrownBy(() -> runJobWithEvents(sinkConfig, Collections.emptyList()))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Unknown Jdbc sink dialect unexpected. Supported dialects are: [mysql]");
    }
}
