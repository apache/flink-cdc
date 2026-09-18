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

package io.debezium.connector.mysql.gtid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.gtid.GtidUtils.computeLatestModeGtidSet;
import static io.debezium.connector.mysql.gtid.GtidUtils.fixRestoredGtidSet;
import static io.debezium.connector.mysql.gtid.GtidUtils.mergeGtidSetInto;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link GtidUtils}. */
class GtidUtilsTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("gtidSetsProvider")
    void testFixingRestoredGtidSet(
            String description, String serverStr, String restoredStr, String expectedStr) {
        MySqlGtidSet serverGtidSet = new MySqlGtidSet(serverStr);
        MySqlGtidSet restoredGtidSet = new MySqlGtidSet(restoredStr);

        MySqlGtidSet result = fixRestoredGtidSet(serverGtidSet, restoredGtidSet);

        assertThat(result).hasToString(expectedStr);
    }

    private static Stream<Arguments> gtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Basic example with a straightforward subset",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-50:63-100",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-50:63-100"),
                Arguments.of(
                        "Multiple intervals with gaps in restored",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:45-80:83-90:92-98",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-80:83-90:92-98"),
                Arguments.of(
                        "Server has disjoint intervals, restored partially overlaps",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-50:60-90:95-200",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:45-50:65-70:96-100",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-50:65-70:96-100"),
                Arguments.of(
                        "Restored partially covers server range",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100:102-200",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:106-150:152-200",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100:102-150:152-200"),
                Arguments.of(
                        "Restored end exceeds server range",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000b:1-200:205-300",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-110,24bc7850-2c16-11e6-a073-0242ac11000b:1-201:210-230:245-305",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000b:1-200:210-230:245-300"),
                Arguments.of(
                        "Multiple UUIDs with different overlaps",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000b:1-50",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:45-80,24bc7850-2c16-11e6-a073-0242ac11000b:30-60,24bc7850-2c16-11e6-a073-0242ac11000c:1-20",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-80,24bc7850-2c16-11e6-a073-0242ac11000b:1-50,24bc7850-2c16-11e6-a073-0242ac11000c:1-20"),
                Arguments.of(
                        "Restored starts after server ends",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:80-150",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100"),
                Arguments.of(
                        "Complex overlapping intervals",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-20:30-50:60-80",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:15-35:45-65:75-85",
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-20:30-35:45-50:60-65:75-80"));
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} for FLINK-39149. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("latestModeGtidSetsProvider")
    void testLatestModeGtidMerge(
            String description,
            String serverGtidStr,
            String checkpointGtidStr,
            String expectedMergedStr) {
        MySqlGtidSet serverGtidSet = new MySqlGtidSet(serverGtidStr);
        MySqlGtidSet checkpointGtidSet = new MySqlGtidSet(checkpointGtidStr);

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        serverGtidSet, new MySqlGtidSet(""), checkpointGtidSet, null);

        assertThat(mergedGtidSet).hasToString(expectedMergedStr);

        // Verify MySQL would not replay pre-checkpoint transactions
        MySqlGtidSet transactionsToSend = serverGtidSet.subtract(mergedGtidSet);
        for (MySqlGtidSet.UUIDSet uuidSet : checkpointGtidSet.getUUIDSets()) {
            String uuid = uuidSet.getUUID();
            long earliestCheckpointTx =
                    uuidSet.getIntervals().stream()
                            .mapToLong(MySqlGtidSet.Interval::getStart)
                            .min()
                            .orElse(1);
            if (earliestCheckpointTx > 1) {
                MySqlGtidSet.UUIDSet toSendUuidSet = transactionsToSend.forServerWithId(uuid);
                if (toSendUuidSet != null) {
                    for (MySqlGtidSet.Interval interval : toSendUuidSet.getIntervals()) {
                        assertThat(interval.getStart())
                                .as(
                                        "Should not replay pre-checkpoint transactions for UUID %s",
                                        uuid)
                                .isGreaterThan(earliestCheckpointTx);
                    }
                }
            }
        }
    }

    private static Stream<Arguments> latestModeGtidSetsProvider() {
        return Stream.of(
                Arguments.of(
                        "Old channel with non-contiguous GTID, new channel present",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000"),
                Arguments.of(
                        "Mixed old channels (contiguous and non-contiguous) with new channel",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000,24bc7850-2c16-11e6-a073-0242ac110003:1-5000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000,24bc7850-2c16-11e6-a073-0242ac110003:1-5000"),
                Arguments.of(
                        "All old channels, no new channels",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000"),
                Arguments.of(
                        "Contiguous checkpoint GTID, no regression",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000"),
                Arguments.of(
                        "Only new channels, checkpoint has unknown UUID",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000",
                        "24bc7850-2c16-11e6-a073-0242ac110009:1-500",
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000,24bc7850-2c16-11e6-a073-0242ac110009:1-500"));
    }

    @Test
    void testMergingGtidSets() {
        MySqlGtidSet base = new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac11000a:1-100");
        MySqlGtidSet toMerge = new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac11000a:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString("24bc7850-2c16-11e6-a073-0242ac11000a:1-100");

        base = new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac11000a:1-100");
        toMerge = new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac11000c:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString(
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000c:1-10");
        base =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000b:1-100");
        toMerge =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-10,24bc7850-2c16-11e6-a073-0242ac11000c:1-10");
        assertThat(mergeGtidSetInto(base, toMerge))
                .hasToString(
                        "24bc7850-2c16-11e6-a073-0242ac11000a:1-100,24bc7850-2c16-11e6-a073-0242ac11000b:1-100,24bc7850-2c16-11e6-a073-0242ac11000c:1-10");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with {@code gtidSourceFilter}. */
    @Test
    void testLatestModeGtidMergeWithSourceFilter() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:1-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000,24bc7850-2c16-11e6-a073-0242ac110003:1-5000");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:5000-8000,24bc7850-2c16-11e6-a073-0242ac110002:1-2000");
        Predicate<String> gtidSourceFilter =
                uuid -> !uuid.equals("24bc7850-2c16-11e6-a073-0242ac110003");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet,
                        new MySqlGtidSet(""),
                        checkpointGtidSet,
                        gtidSourceFilter);

        assertThat(mergedGtidSet.toString())
                .contains("24bc7850-2c16-11e6-a073-0242ac110001:1-8000");
        assertThat(mergedGtidSet.toString())
                .contains("24bc7850-2c16-11e6-a073-0242ac110002:1-2000");
        assertThat(mergedGtidSet.toString()).doesNotContain("24bc7850-2c16-11e6-a073-0242ac110003");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with purged GTID. */
    @Test
    void testLatestModeGtidMergeWithPurgedGtid() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet(
                        "24bc7850-2c16-11e6-a073-0242ac110001:50-10000,24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
        MySqlGtidSet purgedServerGtid =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110001:1-49");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110001:5000-8000");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString())
                .contains("24bc7850-2c16-11e6-a073-0242ac110001:50-8000");
        assertThat(mergedGtidSet.toString())
                .contains("24bc7850-2c16-11e6-a073-0242ac110002:1-3000");

        // Verify no pre-checkpoint replay
        MySqlGtidSet transactionsToSend = availableServerGtidSet.subtract(mergedGtidSet);
        MySqlGtidSet.UUIDSet aaaToSend =
                transactionsToSend.forServerWithId("24bc7850-2c16-11e6-a073-0242ac110001");
        if (aaaToSend != null) {
            for (MySqlGtidSet.Interval interval : aaaToSend.getIntervals()) {
                assertThat(interval.getStart())
                        .as("Should not request pre-checkpoint transactions")
                        .isGreaterThanOrEqualTo(8001);
            }
        }
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with a completely purged UUID. */
    @Test
    void testLatestModeGtidMergeWithFullyPurgedChannel() {
        MySqlGtidSet availableServerGtidSet =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
        MySqlGtidSet purgedServerGtid =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110001:1-500");
        MySqlGtidSet checkpointGtidSet =
                new MySqlGtidSet("24bc7850-2c16-11e6-a073-0242ac110001:200-400");

        MySqlGtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString()).contains("24bc7850-2c16-11e6-a073-0242ac110001:1-400");
        assertThat(mergedGtidSet.toString())
                .contains("24bc7850-2c16-11e6-a073-0242ac110002:1-3000");
    }
}
