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

package org.apache.flink.cdc.connectors.tidb.source.config;

import io.debezium.config.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link TiDBConnectorConfig}. */
class TiDBConnectorConfigTest {

    @Test
    void testBigIntUnsignedHandlingModeDefaultsToPrecise() {
        String configuredMode =
                Configuration.empty().getString(TiDBConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);

        Assertions.assertThat(TiDBConnectorConfig.BigIntUnsignedHandlingMode.parse(configuredMode))
                .isEqualTo(TiDBConnectorConfig.BigIntUnsignedHandlingMode.PRECISE);
    }

    @Test
    void testBigIntUnsignedHandlingModeCanBeSetToLong() {
        Configuration configuration =
                Configuration.create()
                        .with(TiDBConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE.name(), "long")
                        .build();

        Assertions.assertThat(
                        TiDBConnectorConfig.BigIntUnsignedHandlingMode.parse(
                                configuration.getString(
                                        TiDBConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE)))
                .isEqualTo(TiDBConnectorConfig.BigIntUnsignedHandlingMode.LONG);
    }
}
