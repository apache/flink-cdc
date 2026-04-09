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

import java.util.function.Predicate;
import java.util.stream.Stream;

import static io.debezium.connector.mysql.GtidUtils.computeLatestModeGtidSet;
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
                        "Multiple intervals with gaps in restored",
                        "A:1-100",
                        "A:45-80:83-90:92-98",
                        "A:1-80:83-90:92-98"),
                Arguments.of(
                        "Server has disjoint intervals, restored partially overlaps",
                        "A:1-50:60-90:95-200",
                        "A:45-50:65-70:96-100",
                        "A:1-50:65-70:96-100"),
                Arguments.of(
                        "Restored partially covers server range",
                        "A:1-100:102-200",
                        "A:106-150:152-200",
                        "A:1-100:102-150:152-200"),
                Arguments.of(
                        "Restored end exceeds server range",
                        "A:1-100,B:1-200:205-300",
                        "A:1-110,B:1-201:210-230:245-305",
                        "A:1-100,B:1-200:210-230:245-300"),
                Arguments.of(
                        "Multiple UUIDs with different overlaps",
                        "A:1-100,B:1-50",
                        "A:45-80,B:30-60,C:1-20",
                        "A:1-80,B:1-50,C:1-20"),
                Arguments.of("Restored starts after server ends", "A:1-100", "A:80-150", "A:1-100"),
                Arguments.of(
                        "Complex overlapping intervals",
                        "A:1-20:30-50:60-80",
                        "A:15-35:45-65:75-85",
                        "A:1-20:30-35:45-50:60-65:75-80"));
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} for FLINK-39149. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("latestModeGtidSetsProvider")
    void testLatestModeGtidMerge(
            String description,
            String serverGtidStr,
            String checkpointGtidStr,
            String expectedMergedStr) {
        GtidSet serverGtidSet = new GtidSet(serverGtidStr);
        GtidSet checkpointGtidSet = new GtidSet(checkpointGtidStr);

        GtidSet mergedGtidSet =
                computeLatestModeGtidSet(serverGtidSet, new GtidSet(""), checkpointGtidSet, null);

        assertThat(mergedGtidSet).hasToString(expectedMergedStr);

        // Verify MySQL would not replay pre-checkpoint transactions
        GtidSet transactionsToSend = serverGtidSet.subtract(mergedGtidSet);
        for (GtidSet.UUIDSet uuidSet : checkpointGtidSet.getUUIDSets()) {
            String uuid = uuidSet.getUUID();
            long earliestCheckpointTx =
                    uuidSet.getIntervals().stream()
                            .mapToLong(GtidSet.Interval::getStart)
                            .min()
                            .orElse(1);
            if (earliestCheckpointTx > 1) {
                GtidSet.UUIDSet toSendUuidSet = transactionsToSend.forServerWithId(uuid);
                if (toSendUuidSet != null) {
                    for (GtidSet.Interval interval : toSendUuidSet.getIntervals()) {
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
                        "aaa-111:1-10000,bbb-222:1-3000",
                        "aaa-111:5000-8000",
                        "aaa-111:1-8000,bbb-222:1-3000"),
                Arguments.of(
                        "Mixed old channels (contiguous and non-contiguous) with new channel",
                        "aaa-111:1-10000,bbb-222:1-3000,ccc-333:1-5000",
                        "aaa-111:5000-8000,bbb-222:1-2000",
                        "aaa-111:1-8000,bbb-222:1-2000,ccc-333:1-5000"),
                Arguments.of(
                        "All old channels, no new channels",
                        "aaa-111:1-10000,bbb-222:1-3000",
                        "aaa-111:1-8000,bbb-222:1-2000",
                        "aaa-111:1-8000,bbb-222:1-2000"),
                Arguments.of(
                        "Contiguous checkpoint GTID, no regression",
                        "aaa-111:1-10000,bbb-222:1-3000",
                        "aaa-111:1-8000",
                        "aaa-111:1-8000,bbb-222:1-3000"),
                Arguments.of(
                        "Only new channels, checkpoint has unknown UUID",
                        "aaa-111:1-10000,bbb-222:1-3000",
                        "xxx-999:1-500",
                        "aaa-111:1-10000,bbb-222:1-3000,xxx-999:1-500"));
    }

    @Test
    void testMergingGtidSets() {
        GtidSet base = new GtidSet("A:1-100");
        GtidSet toMerge = new GtidSet("A:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100");

        base = new GtidSet("A:1-100");
        toMerge = new GtidSet("C:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100,C:1-10");
        base = new GtidSet("A:1-100,B:1-100");
        toMerge = new GtidSet("A:1-10,C:1-10");
        assertThat(mergeGtidSetInto(base, toMerge)).hasToString("A:1-100,B:1-100,C:1-10");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with {@code gtidSourceFilter}. */
    @Test
    void testLatestModeGtidMergeWithSourceFilter() {
        GtidSet availableServerGtidSet =
                new GtidSet("aaa-111:1-10000,bbb-222:1-3000,ccc-333:1-5000");
        GtidSet checkpointGtidSet = new GtidSet("aaa-111:5000-8000,bbb-222:1-2000");
        Predicate<String> gtidSourceFilter = uuid -> !uuid.equals("ccc-333");

        GtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet,
                        new GtidSet(""),
                        checkpointGtidSet,
                        gtidSourceFilter);

        assertThat(mergedGtidSet.toString()).contains("aaa-111:1-8000");
        assertThat(mergedGtidSet.toString()).contains("bbb-222:1-2000");
        assertThat(mergedGtidSet.toString()).doesNotContain("ccc-333");
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with purged GTID. */
    @Test
    void testLatestModeGtidMergeWithPurgedGtid() {
        GtidSet availableServerGtidSet = new GtidSet("aaa-111:50-10000,bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("aaa-111:1-49");
        GtidSet checkpointGtidSet = new GtidSet("aaa-111:5000-8000");

        GtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString()).contains("aaa-111:50-8000");
        assertThat(mergedGtidSet.toString()).contains("bbb-222:1-3000");

        // Verify no pre-checkpoint replay
        GtidSet transactionsToSend = availableServerGtidSet.subtract(mergedGtidSet);
        GtidSet.UUIDSet aaaToSend = transactionsToSend.forServerWithId("aaa-111");
        if (aaaToSend != null) {
            for (GtidSet.Interval interval : aaaToSend.getIntervals()) {
                assertThat(interval.getStart())
                        .as("Should not request pre-checkpoint transactions")
                        .isGreaterThanOrEqualTo(8001);
            }
        }
    }

    /** Tests {@link GtidUtils#computeLatestModeGtidSet} with a completely purged UUID. */
    @Test
    void testLatestModeGtidMergeWithFullyPurgedChannel() {
        GtidSet availableServerGtidSet = new GtidSet("bbb-222:1-3000");
        GtidSet purgedServerGtid = new GtidSet("aaa-111:1-500");
        GtidSet checkpointGtidSet = new GtidSet("aaa-111:200-400");

        GtidSet mergedGtidSet =
                computeLatestModeGtidSet(
                        availableServerGtidSet, purgedServerGtid, checkpointGtidSet, null);

        assertThat(mergedGtidSet.toString()).contains("aaa-111:1-400");
        assertThat(mergedGtidSet.toString()).contains("bbb-222:1-3000");
    }
}
