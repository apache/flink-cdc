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

package org.apache.flink.cdc.connectors.gaussdb.source.config;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.cdc.connectors.gaussdb.source.config.GaussDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GaussDBSourceConfigTest {

    @Test
    void validatesRequiredOptions() {
        String[] requiredKeys = {
            GaussDBSourceOptions.HOSTNAME.key(),
            GaussDBSourceOptions.USERNAME.key(),
            GaussDBSourceOptions.PASSWORD.key(),
            GaussDBSourceOptions.DATABASE_NAME.key(),
            GaussDBSourceOptions.SLOT_NAME.key()
        };

        for (String missingKey : requiredKeys) {
            Configuration config = requiredConfigExcept(missingKey);
            assertThatThrownBy(() -> GaussDBSourceConfigFactory.fromConfiguration(config))
                    .isInstanceOfAny(NullPointerException.class, IllegalArgumentException.class)
                    .hasMessageContaining(missingKey);
        }

        Configuration blankHostname = requiredConfigExcept("never-missing");
        blankHostname.setString(GaussDBSourceOptions.HOSTNAME.key(), " ");
        assertThatThrownBy(() -> GaussDBSourceConfigFactory.fromConfiguration(blankHostname))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(GaussDBSourceOptions.HOSTNAME.key());
    }

    @Test
    void appliesDefaultValues() {
        Configuration config = requiredConfigExcept("never-missing");
        GaussDBSourceConfig sourceConfig =
                GaussDBSourceConfigFactory.fromConfiguration(config).create(0);

        assertThat(sourceConfig.getPort()).isEqualTo(8000);
        assertThat(sourceConfig.getSchemaList()).containsExactly("public");
        assertThat(sourceConfig.getDecodingPluginName()).isEqualTo("mppdb_decoding");
    }

    @Test
    void generatesJdbcUrl() {
        Configuration config = requiredConfigExcept("never-missing");
        config.setInteger(GaussDBSourceOptions.PORT.key(), 18000);
        config.setString(GaussDBSourceOptions.DATABASE_NAME.key(), "mydb");
        GaussDBSourceConfig sourceConfig =
                GaussDBSourceConfigFactory.fromConfiguration(config).create(0);

        assertThat(sourceConfig.getJdbcUrl()).isEqualTo("jdbc:gaussdb://localhost:18000/mydb");
    }

    @Test
    void scanIncrementalSnapshotEnabledDefaultsToTrue() {
        Configuration config = requiredConfigExcept("never-missing");
        assertThat(config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED)).isTrue();
    }

    @Test
    void scanIncrementalSnapshotEnabledRespectsExplicitTrue() {
        Configuration config = requiredConfigExcept("never-missing");
        config.setBoolean(SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key(), true);
        assertThat(config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED)).isTrue();
    }

    @Test
    void scanIncrementalSnapshotEnabledRespectsExplicitFalse() {
        Configuration config = requiredConfigExcept("never-missing");
        config.setBoolean(SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key(), false);
        assertThat(config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED)).isFalse();
    }

    private static Configuration requiredConfigExcept(String missingKey) {
        Configuration config = new Configuration();
        if (!missingKey.equals(GaussDBSourceOptions.HOSTNAME.key())) {
            config.setString(GaussDBSourceOptions.HOSTNAME.key(), "localhost");
        }
        if (!missingKey.equals(GaussDBSourceOptions.USERNAME.key())) {
            config.setString(GaussDBSourceOptions.USERNAME.key(), "user");
        }
        if (!missingKey.equals(GaussDBSourceOptions.PASSWORD.key())) {
            config.setString(GaussDBSourceOptions.PASSWORD.key(), "pass");
        }
        if (!missingKey.equals(GaussDBSourceOptions.DATABASE_NAME.key())) {
            config.setString(GaussDBSourceOptions.DATABASE_NAME.key(), "db");
        }
        if (!missingKey.equals(GaussDBSourceOptions.SLOT_NAME.key())) {
            config.setString(GaussDBSourceOptions.SLOT_NAME.key(), "slot");
        }
        return config;
    }
}
