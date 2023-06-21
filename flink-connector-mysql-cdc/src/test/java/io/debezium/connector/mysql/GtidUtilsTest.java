/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql;

import org.junit.jupiter.api.Test;

import static io.debezium.connector.mysql.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {
    @Test
    void testFixingRestoredGtidSet() {
        GtidSet serverGtidSet = new GtidSet("A:1-100");
        GtidSet restoredGtidSet = new GtidSet("A:30-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo("A:1-100");

        serverGtidSet = new GtidSet("A:1-100");
        restoredGtidSet = new GtidSet("A:30-50");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo("A:1-50");

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo("A:1-100:102-150,B:20-200");

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150,C:1-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo("A:1-100:102-150,B:20-200,C:1-100");

        serverGtidSet = new GtidSet("A:1-100:102-200,B:20-200");
        restoredGtidSet = new GtidSet("A:106-150:152-200,C:1-100");
        assertThat(fixRestoredGtidSet(serverGtidSet, restoredGtidSet).toString())
                .isEqualTo("A:1-100:102-200,B:20-200,C:1-100");
    }

    @Test
    void testMergingGtidSets() {
        GtidSet base = new GtidSet("A:1-100");
        GtidSet toMerge = new GtidSet("A:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo("A:1-100");

        base = new GtidSet("A:1-100");
        toMerge = new GtidSet("B:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo("A:1-100,B:1-10");

        base = new GtidSet("A:1-100,C:1-100");
        toMerge = new GtidSet("A:1-10,B:1-10");
        assertThat(mergeGtidSetInto(base, toMerge).toString()).isEqualTo("A:1-100,B:1-10,C:1-100");
    }
}
