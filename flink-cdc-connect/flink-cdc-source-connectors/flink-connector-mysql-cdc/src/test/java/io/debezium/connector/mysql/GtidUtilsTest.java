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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("gtidSetsProvider")
    void testFixingRestoredGtidSet(
            String description, String serverStr, String restoredStr, String expectedStr) {
        GtidSet serverGtidSet = new GtidSet(serverStr);
        GtidSet restoredGtidSet = new GtidSet(restoredStr);

        GtidSet result = fixRestoredGtidSet(serverGtidSet, restoredGtidSet);

        assertThat(result.toString()).isEqualTo(expectedStr);
    }

    private static Stream<Arguments> gtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Basic example with a straightforward subset",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-50:63-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-50:63-100"),
                Arguments.of(
                        "Restored starts midrange, single gap",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:45-80",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-80"),
                Arguments.of(
                        "Multiple intervals with gaps in restored",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:45-80:83-90:92-98",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-80:83-90:92-98"),
                Arguments.of(
                        "Server has disjoint intervals, restored partially overlaps",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-50:60-90:95-200",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:45-50:65-70:96-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-50:65-70:96-100"),
                Arguments.of(
                        "Restored completely covers server range",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100"),
                Arguments.of(
                        "Restored partially covers server range",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100:102-200",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:106-150:152-200",
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100:102-150:152-200"));
    }

    @Test
    void testMergingGtidSets() {
        GtidSet base = new GtidSet("e21e8354-04b8-42a9-bfe3-12d71e809836:1-100");
        GtidSet toMerge = new GtidSet("e21e8354-04b8-42a9-bfe3-12d71e809836:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString())
                .isEqualTo("e21e8354-04b8-42a9-bfe3-12d71e809836:1-100");

        base = new GtidSet("e21e8354-04b8-42a9-bfe3-12d71e809836:1-100");
        toMerge = new GtidSet("a32e8354-04b8-42a9-bfe3-12d71e809820:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString())
                .isEqualTo(
                        "a32e8354-04b8-42a9-bfe3-12d71e809820:1-10,e21e8354-04b8-42a9-bfe3-12d71e809836:1-100");

        base =
                new GtidSet(
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-100,c22e8354-04b8-42a9-bfe3-12d71e809831:1-100");
        toMerge =
                new GtidSet(
                        "e21e8354-04b8-42a9-bfe3-12d71e809836:1-10,a32e8354-04b8-42a9-bfe3-12d71e809820:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString())
                .isEqualTo(
                        "a32e8354-04b8-42a9-bfe3-12d71e809820:1-10,c22e8354-04b8-42a9-bfe3-12d71e809831:1-100,e21e8354-04b8-42a9-bfe3-12d71e809836:1-100");
    }
}
