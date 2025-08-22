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

package io.debezium.connector.mysql;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("gtidSetsProvider")
    void testFixingRestoredGtidSet(
            String description, String serverStr, String restoredStr, String expectedStr) {
        GtidSet serverGtidSet = new GtidSet(serverStr);
        GtidSet restoredGtidSet = new GtidSet(restoredStr);

        GtidSet result = fixRestoredGtidSet(serverGtidSet, restoredGtidSet);

        assertThat(result).hasToString(expectedStr);
    }

    private static Stream<Arguments> gtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Basic example with a straightforward subset",
                        "A:1-100",
                        "A:1-50:63-100",
                        "A:1-50:63-100"),
                Arguments.of(
                        "Restored starts midrange, single gap", "A:1-100", "A:45-80", "A:1-80"),
                Arguments.of(
                        "Multiple intervals with gaps in restored",
                        "A:1-100,B:1-100",
                        "A:45-80:83-90:92-98,C:1-20",
                        "A:1-80:83-90:92-98,B:1-100,C:1-20"),
                Arguments.of(
                        "Server has disjoint intervals, restored partially overlaps",
                        "A:1-50:60-90:95-200",
                        "A:45-50:65-70:96-100",
                        "A:1-50:65-70:96-100"),
                Arguments.of(
                        "Restored completely covers server range", "A:1-100", "A:1-100", "A:1-100"),
                Arguments.of(
                        "Restored partially covers server range",
                        "A:1-100:102-200",
                        "A:106-150:152-200",
                        "A:1-100:102-150:152-200"),
                Arguments.of("Restored end exceeds server range", "A:1-100", "A:1-110", "A:1-100"));
    }

    @Test
    void testMergingGtidSets() {
        GtidSet base = new GtidSet("A:1-100");
        GtidSet toMerge = new GtidSet("A:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100");

        base = new GtidSet("A:1-100");
        toMerge = new GtidSet("B:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100,B:1-10");

        base = new GtidSet("A:1-100,C:1-100");
        toMerge = new GtidSet("A:1-10,B:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100,B:1-10,C:1-100");
    }
}
