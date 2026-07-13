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

package org.apache.flink.cdc.connectors.sqlserver.source.config;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link SqlServerSourceConfigFactory}. */
class SqlServerSourceConfigFactoryTest {

    @Test
    void testTimestampStartupOption() {
        long startupTimestampMillis = 1667232000000L;

        SqlServerSourceConfigFactory factory = new SqlServerSourceConfigFactory();
        factory.hostname("localhost")
                .port(1433)
                .databaseList("inventory")
                .tableList("inventory.dbo.products")
                .username("flinkuser")
                .password("flinkpw")
                .serverTimeZone("UTC");
        factory.startupOptions(StartupOptions.timestamp(startupTimestampMillis));

        SqlServerSourceConfig sourceConfig = factory.create(0);

        Assertions.assertThat(sourceConfig.getStartupOptions())
                .isEqualTo(StartupOptions.timestamp(startupTimestampMillis));
        Assertions.assertThat(sourceConfig.getDbzProperties().getProperty("snapshot.mode"))
                .isEqualTo("schema_only");
    }
}
