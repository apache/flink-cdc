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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utils for handling GTIDs. */
public class GtidUtils {

    /**
     * This method corrects the GTID set that has been restored from a state or checkpoint using the
     * GTID set fetched from the server via SHOW MASTER STATUS. During the correction process, the
     * restored GTID set is adjusted according to the server's GTID set to ensure it does not exceed
     * the latter. For each UUID in the restored GTID set, if it exists in the server's GTID set,
     * then it will be adjusted according to the server's GTID set; if it does not exist in the
     * server's GTID set, it will be directly added to the new GTID set.
     */
    public static GtidSet fixRestoredGtidSet(GtidSet serverGtidSet, GtidSet restoredGtidSet) {
        Map<String, GtidSet.UUIDSet> newSet = new HashMap<>();
        serverGtidSet.getUUIDSets().forEach(uuidSet -> newSet.put(uuidSet.getUUID(), uuidSet));
        for (GtidSet.UUIDSet restoredUuidSet : restoredGtidSet.getUUIDSets()) {
            GtidSet.UUIDSet serverUuidSet = newSet.get(restoredUuidSet.getUUID());
            if (serverUuidSet != null) {
                List<GtidSet.Interval> serverIntervals = serverUuidSet.getIntervals();
                List<GtidSet.Interval> restoredIntervals = restoredUuidSet.getIntervals();

                long earliestRestoredTx = getMinIntervalStart(restoredIntervals);

                List<com.github.shyiko.mysql.binlog.GtidSet.Interval> merged = new ArrayList<>();

                // Process each server interval
                for (GtidSet.Interval serverInterval : serverIntervals) {
                    // First, check if any part comes before earliest restored
                    if (serverInterval.getStart() < earliestRestoredTx) {
                        long end = Math.min(serverInterval.getEnd(), earliestRestoredTx - 1);
                        merged.add(
                                new com.github.shyiko.mysql.binlog.GtidSet.Interval(
                                        serverInterval.getStart(), end));
                    }

                    // Then check for overlaps with restored intervals
                    for (GtidSet.Interval restoredInterval : restoredIntervals) {
                        if (serverInterval.getStart() <= restoredInterval.getEnd()
                                && serverInterval.getEnd() >= restoredInterval.getStart()) {
                            // There's an overlap - add the intersection
                            long intersectionStart =
                                    Math.max(
                                            serverInterval.getStart(), restoredInterval.getStart());
                            long intersectionEnd =
                                    Math.min(serverInterval.getEnd(), restoredInterval.getEnd());

                            if (intersectionStart <= intersectionEnd) {
                                merged.add(
                                        new com.github.shyiko.mysql.binlog.GtidSet.Interval(
                                                intersectionStart, intersectionEnd));
                            }
                        }
                    }
                }

                GtidSet.UUIDSet mergedUuidSet =
                        new GtidSet.UUIDSet(
                                new com.github.shyiko.mysql.binlog.GtidSet.UUIDSet(
                                        restoredUuidSet.getUUID(), merged));

                newSet.put(restoredUuidSet.getUUID(), mergedUuidSet);
            } else {
                newSet.put(restoredUuidSet.getUUID(), restoredUuidSet);
            }
        }
        return new GtidSet(newSet);
    }

    private static long getMinIntervalStart(List<GtidSet.Interval> intervals) {
        return Collections.min(intervals, Comparator.comparingLong(GtidSet.Interval::getStart))
                .getStart();
    }

    /**
     * This method merges one GTID set (toMerge) into another (base), without overwriting the
     * existing elements in the base GTID set.
     */
    public static GtidSet mergeGtidSetInto(GtidSet base, GtidSet toMerge) {
        Map<String, GtidSet.UUIDSet> newSet = new HashMap<>();
        base.getUUIDSets().forEach(uuidSet -> newSet.put(uuidSet.getUUID(), uuidSet));
        for (GtidSet.UUIDSet uuidSet : toMerge.getUUIDSets()) {
            if (!newSet.containsKey(uuidSet.getUUID())) {
                newSet.put(uuidSet.getUUID(), uuidSet);
            }
        }
        return new GtidSet(newSet);
    }
}
