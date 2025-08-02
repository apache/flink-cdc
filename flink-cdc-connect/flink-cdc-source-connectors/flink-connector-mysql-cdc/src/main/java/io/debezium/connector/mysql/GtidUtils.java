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

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Utils for handling GTIDs. */
public class GtidUtils {
    private static final Logger LOG = LoggerFactory.getLogger(GtidUtils.class);

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
        for (GtidSet.UUIDSet uuidSet : restoredGtidSet.getUUIDSets()) {
            GtidSet.UUIDSet serverUuidSet = newSet.get(uuidSet.getUUID());
            if (serverUuidSet != null) {
                long serverIntervalEnd = getIntervalEnd(serverUuidSet);

                // The end of the restored interval cannot be greater than the end of the server
                // span
                RangeSet<Long> restoredRangeSet = TreeRangeSet.create();
                for (GtidSet.Interval restoredInterval : uuidSet.getIntervals()) {
                    if (restoredInterval.getStart() > serverIntervalEnd) {
                        LOG.warn(
                                "Discard restored interval {} because it is greater than the end of the server span {}",
                                restoredInterval,
                                serverIntervalEnd);
                    } else if (restoredInterval.getEnd() > serverIntervalEnd) {
                        Range<Long> adjustedInterval =
                                Range.closed(restoredInterval.getStart(), serverIntervalEnd);
                        restoredRangeSet.add(adjustedInterval);
                        LOG.warn(
                                "Adjust restored interval {} to {} because it is greater than the end of the server span {}",
                                restoredInterval,
                                adjustedInterval,
                                serverIntervalEnd);
                    } else {
                        restoredRangeSet.add(
                                Range.closed(
                                        restoredInterval.getStart(), restoredInterval.getEnd()));
                    }
                }

                // Add the server intervals that do not overlap with the middle gap of the restored
                // intervals
                final Range<Long> restoredSmallestRange =
                        restoredRangeSet.asRanges().iterator().next();
                serverUuidSet.getIntervals().stream()
                        .filter(
                                interval ->
                                        interval.getStart()
                                                <= restoredSmallestRange.upperEndpoint())
                        .forEach(
                                interval ->
                                        restoredRangeSet.add(
                                                Range.closed(
                                                        interval.getStart(),
                                                        Math.min(
                                                                interval.getEnd(),
                                                                restoredSmallestRange
                                                                        .upperEndpoint()))));

                List<com.github.shyiko.mysql.binlog.GtidSet.Interval> newIntervals =
                        restoredRangeSet.asRanges().stream()
                                .map(
                                        range ->
                                                new com.github.shyiko.mysql.binlog.GtidSet.Interval(
                                                        range.lowerEndpoint(),
                                                        range.upperEndpoint()))
                                .collect(Collectors.toList());

                newSet.put(
                        uuidSet.getUUID(),
                        new GtidSet.UUIDSet(
                                new com.github.shyiko.mysql.binlog.GtidSet.UUIDSet(
                                        uuidSet.getUUID(), newIntervals)));
            } else {
                newSet.put(uuidSet.getUUID(), uuidSet);
            }
        }
        return new GtidSet(newSet);
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

    private static long getIntervalEnd(GtidSet.UUIDSet uuidSet) {
        return uuidSet.getIntervals().stream()
                .mapToLong(GtidSet.Interval::getEnd)
                .max()
                .getAsLong();
    }
}
