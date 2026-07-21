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

/**
 * Unit tests for {@link MariaDbGtidStrategy}. MariaDB GTIDs are "domain-server-sequence" and the
 * sequence advances per domain across server, so comparison must key on the domain and ignore the
 * server id - otherwise a post-failover event (same domain, new server id) looks like a different
 * stream and the offset is mishandled (Debezium dbz#1672, Flink CDC issue #2929)
 */
class MariaDbGtidStrategyTest {

    private final MariaDbGtidStrategy strategy = new MariaDbGtidStrategy();

    @Test
    void dialectIsMariadb() {
        assertThat(strategy.dialect()).isEqualTo("mariadb");
        assertThat(GtidStrategies.of("mariadb")).isInstanceOf(MariaDbGtidStrategy.class);
        assertThat(GtidStrategies.of("Mariadb")).isInstanceOf(MariaDbGtidStrategy.class);
    }

    @Test
    void canParseMariaDbGtid() {
        assertThat(strategy.canParse("0-1-100")).isTrue();
        assertThat(strategy.canParse("0-1-100,1-2-50")).isTrue();
        // MySQL uuid:interval form MUST NOT be claimed by the MariaDB parser.
        assertThat(strategy.canParse("A:1-100")).isFalse();
        assertThat(strategy.canParse(null)).isFalse();
    }

    @Test
    void detectRoutesByGtidText() {
        assertThat(GtidStrategies.detect("0-1-100")).isInstanceOf(MariaDbGtidStrategy.class);
        assertThat(GtidStrategies.detect("0-1-100,1-2-50")).isInstanceOf(MariaDbGtidStrategy.class);
        // uuid:interval text stays on the MySQL strategy
        assertThat(GtidStrategies.detect("A:1-100")).isInstanceOf(MysqlGtidStrategy.class);
    }

    @Test
    void isEqualIgnoresServerId() {
        // Same domain + same sequence, different server id (failover) -> equal
        assertThat(strategy.isEqual("0-1-100", "0-2-100")).isTrue();

        // different sequence
        assertThat(strategy.isEqual("0-1-100", "0-1-101")).isFalse();
        // different domain
        assertThat(strategy.isEqual("0-1-100", "1-1-100")).isFalse();
    }

    @Test
    void isEqualHandlesMultipleDomainsOrderIndependently() {
        assertThat(strategy.isEqual("0-1-100,1-2-50", "1-9-50,0-7-100")).isTrue();
        assertThat(strategy.isEqual("0-1-100,1-2-50", "0-1-100")).isFalse();
    }

    @Test
    void isContainedWithinKeysOnDomainAndIgnoresServerId() {
        // Failover: sub at seq 100 on server 1, sup advanced to seq 102 on server 2 -> contained
        assertThat(strategy.isContainedWithin("0-1-100", "0-2-102")).isTrue();
        assertThat(strategy.isContainedWithin("0-1-100", "0-2-100")).isTrue();

        // sub ahead of sup -> not contained.
        assertThat(strategy.isContainedWithin("0-1-101", "0-2-100")).isFalse();
        // domain missing
        assertThat(strategy.isContainedWithin("0-1-100,1-1-1", "0-2-100")).isFalse();
        // multi-domain
        assertThat(strategy.isContainedWithin("0-1-100,1-1-50", "0-2-100,1-9-60")).isTrue();
    }

    @Test
    void emptyGtidSets() {
        assertThat(strategy.isEqual("", "")).isTrue();
        assertThat(strategy.isContainedWithin("", "0-1-100")).isTrue();
        assertThat(strategy.isContainedWithin("0-1-100", "")).isFalse();
    }
}
