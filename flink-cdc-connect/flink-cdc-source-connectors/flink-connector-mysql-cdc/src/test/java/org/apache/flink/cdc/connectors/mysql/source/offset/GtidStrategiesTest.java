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

package org.apache.flink.cdc.connectors.mysql.source.offset;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GtidStrategies}. */
class GtidStrategiesTest {

    private static final String MARIADB_VERSION = "5.5.5-10.6.12-MariaDB-1:10.6.12+maria~ubu2005";
    private static final String MYSQL_VERSION = "8.0.32";

    @Test
    void autoConstantIsAuto() {
        assertThat(GtidStrategies.AUTO).isEqualTo("auto");
    }

    @Test
    void pinnedDialectAlwaysWinsRegardlessOfVersion() {
        assertThat(GtidStrategies.resolveDialect("mysql", MARIADB_VERSION)).isEqualTo("mysql");
        assertThat(GtidStrategies.resolveDialect("MySQL", MYSQL_VERSION)).isEqualTo("mysql");

        assertThat(GtidStrategies.resolveDialect("MariaDB", MARIADB_VERSION)).isEqualTo("mariadb");
        assertThat(GtidStrategies.resolveDialect("mariadb", MYSQL_VERSION)).isEqualTo("mariadb");
    }

    @Test
    void autoDetectVersionBanner() {
        assertThat(GtidStrategies.resolveDialect("auto", MARIADB_VERSION)).isEqualTo("mariadb");
        assertThat(GtidStrategies.resolveDialect("auto", "10.11.2-MARIADB")).isEqualTo("mariadb");
        assertThat(GtidStrategies.resolveDialect("auto", MYSQL_VERSION)).isEqualTo("mysql");
        assertThat(GtidStrategies.resolveDialect("AUTO", "5.7.41-log")).isEqualTo("mysql");
    }

    @Test
    void absentOrUnknownDefaultToMysql() {
        assertThat(GtidStrategies.resolveDialect("auto", null)).isEqualTo("mysql");
        assertThat(GtidStrategies.resolveDialect("auto", "")).isEqualTo("mysql");

        // null/unknown configured behaves like auto
        assertThat(GtidStrategies.resolveDialect(null, MYSQL_VERSION)).isEqualTo("mysql");
        assertThat(GtidStrategies.resolveDialect(null, MARIADB_VERSION)).isEqualTo("mariadb");
        assertThat(GtidStrategies.resolveDialect("postgres", MARIADB_VERSION)).isEqualTo("mariadb");
        assertThat(GtidStrategies.resolveDialect(null, null)).isEqualTo("mysql");
    }
}
