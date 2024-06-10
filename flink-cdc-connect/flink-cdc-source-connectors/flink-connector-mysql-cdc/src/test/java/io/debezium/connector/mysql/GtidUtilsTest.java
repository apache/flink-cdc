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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {

    @ParameterizedTest
    @CsvSource(
            value = { //
                "A:1-100;A:30-100;A:1-100", //
                "A:1-100;A:30-50;A:1-50", //
                "A:1-100:102-200,B:20-200;A:106-150;A:1-100:102-150,B:20-200", //
                "A:1-100:102-200,B:20-200;A:106-150,C:1-100;A:1-100:102-150,B:20-200,C:1-100", //
                "A:1-100:102-200,B:20-200;A:106-150:152-200,C:1-100;A:1-100:102-200,B:20-200,C:1-100", //
            },
            delimiter = ';')
    void testFixingRestoredGtidSet(
            final String serverGtidValue,
            final String restoredGtidValue,
            final String expectedValue) {
        GtidSet serverGtidSet = new GtidSet(serverGtidValue);
        GtidSet restoredGtidSet = new GtidSet(restoredGtidValue);
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet)).hasToString(expectedValue);
    }

    @ParameterizedTest
    @CsvSource(
            value = { //
                "A:1-100;A:1-10;A:1-100", //
                "A:1-100;B:1-10;A:1-100,B:1-10", //
                "A:1-100,C:1-100;A:1-10,B:1-10;A:1-100,B:1-10,C:1-100" //
            },
            delimiter = ';')
    void testMergingGtidSets(
            final String baseGtidValue, final String mergedGtidValue, final String expectedValue) {
        GtidSet base = new GtidSet(baseGtidValue);
        GtidSet toMerge = new GtidSet(mergedGtidValue);
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString(expectedValue);
    }
}
