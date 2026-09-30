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

package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.util.FlinkRuntimeException;

import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests single-table validation without requiring a TiDB cluster. */
class TiDBDialectValidationTest {

    @Test
    void shouldRejectAFilterThatDiscoversMultipleTables() {
        assertThatThrownBy(
                        () ->
                                TiDBDialect.validateSingleDiscoveredTable(
                                        Arrays.asList(
                                                new TableId("inventory", null, "products"),
                                                new TableId("inventory", null, "customers")),
                                        Collections.singletonList("inventory\\..*")))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("exactly one table")
                .hasMessageContaining("2 tables");
    }
}
