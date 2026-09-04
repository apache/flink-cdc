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

import io.debezium.connector.mysql.GtidSet;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit Test for {@link MysqlGtidStrategy}. This strategy MUST BE equivalent to the inline {@code
 * new GtidSet.equals()/isContanedWithin()} logic that {@link BinlogOffset} used before the {@link
 * GtidStrategy} abstraction was extracted.
 */
class MysqlGtidStrategyTest {

    private final MysqlGtidStrategy strategy = new MysqlGtidStrategy();

    // old (reference) impl: exactly what BinlogOffset.compareTo() did
    private static boolean oldIsEqual(String a, String b) {
        return new GtidSet(a).equals(new GtidSet(b));
    }

    private static boolean oldIsContainedWithin(String sub, String sup) {
        return new GtidSet(sub).isContainedWithin(new GtidSet(sup));
    }

    @Test
    void dialectIsMysql() {
        assertThat(strategy.dialect()).isEqualTo("mysql");
        assertThat(GtidStrategies.of("mysql")).isInstanceOf(MysqlGtidStrategy.class);
        assertThat(GtidStrategies.of(null)).isInstanceOf(MysqlGtidStrategy.class);
    }

    @Test
    void detectReturnsMysqlForUuidInterval() {
        assertThat(GtidStrategies.detect("A:1-100")).isInstanceOf(MysqlGtidStrategy.class);
    }

    @Test
    void canParseMysqlGtidText() {
        assertThat(strategy.canParse("A:1-100")).isTrue();
        assertThat(strategy.canParse("a:1-100:102-200,B:20-200")).isTrue();
        assertThat(strategy.canParse("")).isTrue();
        assertThat(strategy.canParse(null)).isFalse();
    }

    @Test
    void isEqualMatchesOldImpl() {
        String[][] pairs = {
            {"A:1-100", "A:1-100"},
            {"A:1-100", "A:1-50"},
            {"A:1-100", "B:1-100"},
            {"A:1-100:102-200,B:20-200", "A:1-100:102-200,B:20-200"},
            {"A:1-100,B:1-10", "B:1-10,A:1-100"},
            {"A:1-100", "A:1-100,C:1-5"}
        };
        for (String[] pair : pairs) {
            assertThat(strategy.isEqual(pair[0], pair[1]))
                    .as("isEqual(%s,%s)", pair[0], pair[1])
                    .isEqualTo(oldIsEqual(pair[0], pair[1]));
        }
    }

    @Test
    void isContainedWithinMatchesOldImpl() {
        String[][] pairs = {
            {"A:1-50", "A:1-100"},
            {"A:1-100", "A:1-100"},
            {"A:1-100", "A:1-50"},
            {"A:1-100", "A:1-100:102-200,B:20-200"},
            {"A:1-100,C:1-5", "A:1-100"},
            {"B:1-10", "A:1-100,B:1-10"},
        };
        for (String[] pair : pairs) {
            assertThat(strategy.isContainedWithin(pair[0], pair[1]))
                    .as("isContainedWithin(%s,%s)", pair[0], pair[1])
                    .isEqualTo(oldIsContainedWithin(pair[0], pair[1]));
        }
    }

    @Test
    void fuzzCrossValidationAgainstOldImpl() {
        Random random = new Random(42);
        for (int i = 0; i < 2000; i++) {
            String a = randomGtid(random);
            String b = randomGtid(random);

            assertThat(strategy.isEqual(a, b))
                    .as("isEqual(%s,%s)", a, b)
                    .isEqualTo(oldIsEqual(a, b));
            assertThat(strategy.isContainedWithin(a, b))
                    .as("isContainedWithin(%s,%s)", a, b)
                    .isEqualTo(oldIsContainedWithin(a, b));
        }
    }

    private static String randomGtid(Random random) {
        // Build an uuid:interval set over a small server
        char[] servers = {'A', 'B', 'C'};
        int count = 1 + random.nextInt(servers.length);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(",");
            }
            int start = 1 + random.nextInt(50);
            int end = start + random.nextInt(50);
            sb.append(servers[i]).append(":").append(start).append("-").append(end);
        }
        return sb.toString();
    }
}
