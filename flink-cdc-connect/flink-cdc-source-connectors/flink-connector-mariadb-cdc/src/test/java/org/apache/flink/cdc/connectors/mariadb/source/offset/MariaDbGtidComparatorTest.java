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

package org.apache.flink.cdc.connectors.mariadb.source.offset;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link MariaDbGtidComparator}. MariaDB GTIDs are {@code domain-server-sequence}
 * and the sequence advances per domain across servers, so comparison must key on the domain and
 * ignore the server id — otherwise a post-failover event (same domain, new server id) looks like a
 * different stream and the offset is mishandled (Debezium DBZ-1672, Flink CDC #2929).
 */
class MariaDbGtidComparatorTest {

    @Test
    void canParseMariaDbGtid() {
        assertThat(MariaDbGtidComparator.canParse("0-1-100")).isTrue();
        assertThat(MariaDbGtidComparator.canParse("0-1-100,1-2-50")).isTrue();
        // MySQL uuid:interval form MUST NOT be claimed by the MariaDB parser.
        assertThat(MariaDbGtidComparator.canParse("A:1-100")).isFalse();
        assertThat(MariaDbGtidComparator.canParse(null)).isFalse();
    }

    @Test
    void isEqualIgnoresServerId() {
        // Same domain + same sequence, different server id (failover) -> equal
        assertThat(MariaDbGtidComparator.isEqual("0-1-100", "0-2-100")).isTrue();
        // different sequence
        assertThat(MariaDbGtidComparator.isEqual("0-1-100", "0-1-101")).isFalse();
        // different domain
        assertThat(MariaDbGtidComparator.isEqual("0-1-100", "1-1-100")).isFalse();
    }

    @Test
    void isEqualHandlesMultipleDomainsOrderIndependently() {
        assertThat(MariaDbGtidComparator.isEqual("0-1-100,1-2-50", "1-9-50,0-7-100")).isTrue();
        assertThat(MariaDbGtidComparator.isEqual("0-1-100,1-2-50", "0-1-100")).isFalse();
    }

    @Test
    void isContainedWithinKeysOnDomainAndIgnoresServerId() {
        // Failover: sub at seq 100 on server 1, sup advanced to seq 102 on server 2 -> contained
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-100", "0-2-102")).isTrue();
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-100", "0-2-100")).isTrue();
        // sub ahead of sup -> not contained.
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-101", "0-2-100")).isFalse();
        // domain missing
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-100,1-1-1", "0-2-100")).isFalse();
        // multi-domain
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-100,1-1-50", "0-2-100,1-9-60"))
                .isTrue();
    }

    @Test
    void emptyGtidSets() {
        assertThat(MariaDbGtidComparator.isEqual("", "")).isTrue();
        assertThat(MariaDbGtidComparator.isContainedWithin("", "0-1-100")).isTrue();
        assertThat(MariaDbGtidComparator.isContainedWithin("0-1-100", "")).isFalse();
    }

    /**
     * The comparison methods reject non-MariaDB GTID text rather than silently mis-comparing it.
     * Callers that may see untrusted input (a restored offset, a user-supplied specific-offset)
     * must guard with {@link MariaDbGtidComparator#canParse(String)} first and raise an actionable
     * error; {@code StatefulTaskContext#checkMariadbGtidSet} does exactly that.
     */
    @Test
    void nonMariaDbGtidTextIsRejected() {
        assertThatThrownBy(() -> MariaDbGtidComparator.isEqual("abcd:1-4", "0-1-100"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid MariaDB gtid format");

        assertThatThrownBy(() -> MariaDbGtidComparator.isContainedWithin("0-1", "0-1-100"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid MariaDB gtid format");
    }
}
