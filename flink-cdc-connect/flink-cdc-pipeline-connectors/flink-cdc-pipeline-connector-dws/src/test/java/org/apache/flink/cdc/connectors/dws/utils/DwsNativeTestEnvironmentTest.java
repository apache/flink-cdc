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

package org.apache.flink.cdc.connectors.dws.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the opt-in external DWS integration-test environment. */
class DwsNativeTestEnvironmentTest {

    @Test
    void shouldFailWhenExplicitEnvironmentIsIncomplete() {
        Map<String, String> environment = validEnvironment();
        environment.remove(DwsNativeTestEnvironment.PASSWORD_ENV);

        assertThatThrownBy(() -> DwsNativeTestEnvironment.from(environment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(DwsNativeTestEnvironment.PASSWORD_ENV)
                .hasMessageNotContaining("test-secret");
    }

    @Test
    void shouldOnlyAcceptTablesOwnedByTheCurrentFixture() {
        DwsNativeTestEnvironment environment =
                DwsNativeTestEnvironment.from(validEnvironment(), "run_42");

        String owned = environment.ownedTable("orders");

        assertThat(owned).isEqualTo("test_schema.flink_cdc_dws_run_42_orders");
        assertThat(environment.isOwnedTable(owned)).isTrue();
        assertThat(environment.isOwnedTable("test_schema.orders")).isFalse();
        assertThat(environment.isOwnedTable("other.flink_cdc_dws_run_42_orders")).isFalse();
        assertThat(environment.toString())
                .doesNotContain("test-secret")
                .doesNotContain("jdbc:postgresql://secret-host:5432/test");
    }

    @Test
    void shouldRejectUnsafeLogicalTableNames() {
        DwsNativeTestEnvironment environment =
                DwsNativeTestEnvironment.from(validEnvironment(), "run_42");

        assertThatThrownBy(() -> environment.ownedTable("orders; drop table users"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static Map<String, String> validEnvironment() {
        Map<String, String> values = new HashMap<>();
        values.put(
                DwsNativeTestEnvironment.JDBC_URL_ENV, "jdbc:postgresql://secret-host:5432/test");
        values.put(DwsNativeTestEnvironment.USERNAME_ENV, "test-user");
        values.put(DwsNativeTestEnvironment.PASSWORD_ENV, "test-secret");
        values.put(DwsNativeTestEnvironment.SCHEMA_ENV, "test_schema");
        return values;
    }
}
