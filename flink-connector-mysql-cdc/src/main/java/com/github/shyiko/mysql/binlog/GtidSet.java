/*
 * Copyright 2015 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog;

import java.util.*;

/**
 * Copied from https://github.com/osheroff/mysql-binlog-connector-java project to fix
 * https://github.com/ververica/flink-cdc-connectors/issues/1944
 *
 * <p>Line 106 ~ 120: A new method called "overwriteThenAdd" has been added to address the incomplete Gtid storage
 * issue in MySQL-CDC when storing checkpoints. In the MySQL-CDC scenario, consuming from a certain GTID should be
 * ordered, and GTIDs before this should not be consumed again. Therefore, it is assumed that when recording a
 * checkpoint, the historical offset should also be recorded as processed.
 */
public class GtidSet {

    private final Map<String, UUIDSet> map = new LinkedHashMap<String, UUIDSet>();

    /**
     * @param gtidSet gtid set comprised of closed intervals (like MySQL's executed_gtid_set).
     */
    public GtidSet(String gtidSet) {
        String[] uuidSets = (gtidSet == null || gtidSet.isEmpty()) ? new String[0] :
                gtidSet.replace("\n", "").split(",");
        for (String uuidSet : uuidSets) {
            int uuidSeparatorIndex = uuidSet.indexOf(":");
            String sourceId = uuidSet.substring(0, uuidSeparatorIndex);
            List<Interval> intervals = new ArrayList<Interval>();
            String[] rawIntervals = uuidSet.substring(uuidSeparatorIndex + 1).split(":");
            for (String interval : rawIntervals) {
                String[] is = interval.split("-");
                long[] split = new long[is.length];
                for (int i = 0, e = is.length; i < e; i++) {
                    split[i] = Long.parseLong(is[i]);
                }
                if (split.length == 1) {
                    split = new long[] {split[0], split[0]};
                }
                intervals.add(new Interval(split[0], split[1]));
            }
            map.put(sourceId, new UUIDSet(sourceId, intervals));
        }
    }

    /**
     * Get an immutable collection of the {@link UUIDSet range of GTIDs for a single server}.
     * @return the {@link UUIDSet GTID ranges for each server}; never null
     */
    public Collection<UUIDSet> getUUIDSets() {
        return Collections.unmodifiableCollection(map.values());
    }

    /**
     * Find the {@link UUIDSet} for the server with the specified UUID.
     * @param uuid the UUID of the server
     * @return the {@link UUIDSet} for the identified server, or {@code null} if there are no GTIDs from that server.
     */
    public UUIDSet getUUIDSet(String uuid) {
        return map.get(uuid);
    }

    /**
     * Add or replace the UUIDSet
     * @param uuidSet UUIDSet to be added
     * @return the old {@link UUIDSet} for the server given in uuidSet param,
     *         or {@code null} if there are no UUIDSet for the given server.
     */
    public UUIDSet putUUIDSet(UUIDSet uuidSet) {
        return map.put(uuidSet.getUUID(), uuidSet);
    }

    /**
     * @param gtid GTID ("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already there)
     */
    public boolean add(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        UUIDSet uuidSet = map.get(sourceId);
        if (uuidSet == null) {
            map.put(sourceId, uuidSet = new UUIDSet(sourceId, new ArrayList<Interval>()));
        }
        return uuidSet.add(transactionId);
    }

    /**
     * Add a Gtid string to the current GtidSet. If the corresponding sourceId is being added for the first time,
     * overwrite it; otherwise, add it.
     * Overwrite: For example, if the current GtidSet is A:1-100, and a Gtid A:150 is added with overwrite, then the
     * current GtidSet will become A:1-150.
     * @param gtid GTID ("source_id:transaction_id")
     * @return whether or not gtid was added to the set (false if it was already there)
     */
    public boolean overwriteThenAdd(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        UUIDSet uuidSet = map.get(sourceId);
        if (uuidSet == null) {
            map.put(sourceId, uuidSet = new UUIDSet(sourceId, new ArrayList<>()));
        }
        if (!uuidSet.isOverwrited) {
            uuidSet.overwrite(transactionId);
            return true;
        } else {
            return uuidSet.add(transactionId);
        }
    }

    /**
     * Determine if the GTIDs represented by this object are contained completely within the supplied set of GTIDs.
     * Note that if two {@link GtidSet}s are equal, then they both are subsets of the other.
     * @param other the other set of GTIDs; may be null
     * @return {@code true} if all of the GTIDs in this set are equal to or completely contained within the supplied
     * set of GTIDs, or {@code false} otherwise
     */
    public boolean isContainedWithin(GtidSet other) {
        if (other == null) {
            return false;
        }
        if (this == other) {
            return true;
        }
        if (this.equals(other)) {
            return true;
        }
        for (UUIDSet uuidSet : map.values()) {
            UUIDSet thatSet = other.getUUIDSet(uuidSet.getUUID());
            if (!uuidSet.isContainedWithin(thatSet)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return map.keySet().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GtidSet) {
            GtidSet that = (GtidSet) obj;
            return this.map.equals(that.map);
        }
        return false;
    }

    @Override
    public String toString() {
        List<String> gtids = new ArrayList<String>();
        for (UUIDSet uuidSet : map.values()) {
            gtids.add(uuidSet.getUUID() + ":" + join(uuidSet.intervals, ":"));
        }
        return join(gtids, ",");
    }

    private static String join(Collection<?> o, String delimiter) {
        if (o.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Object o1 : o) {
            sb.append(o1).append(delimiter);
        }
        return sb.substring(0, sb.length() - delimiter.length());
    }

    /**
     * A range of GTIDs for a single server with a specific UUID.
     * @see GtidSet
     */
    public static final class UUIDSet {

        private String uuid;
        private List<Interval> intervals;
        private boolean isOverwrited = false;

        public UUIDSet(String uuid, List<Interval> intervals) {
            this.uuid = uuid;
            this.intervals = intervals;
            if (intervals.size() > 1) {
                joinAdjacentIntervals(0);
            }
        }

        private void overwrite(long transactionId) {
            intervals = new ArrayList<>(Collections.singletonList(new Interval(1, transactionId)));
            isOverwrited = true;
        }

        private boolean add(long transactionId) {
            int index = findInterval(transactionId);
            boolean addedToExisting = false;
            if (index < intervals.size()) {
                Interval interval = intervals.get(index);
                if (interval.start == transactionId + 1) {
                    interval.start = transactionId;
                    addedToExisting = true;
                } else
                if (interval.end + 1 == transactionId) {
                    interval.end = transactionId;
                    addedToExisting = true;
                } else
                if (interval.start <= transactionId && transactionId <= interval.end) {
                    return false;
                }
            }
            if (!addedToExisting) {
                intervals.add(index, new Interval(transactionId, transactionId));
            }
            if (intervals.size() > 1) {
                joinAdjacentIntervals(index);
            }
            return true;
        }

        /**
         * Collapses intervals like a-(b-1):b-c into a-c (only in index+-1 range).
         */
        private void joinAdjacentIntervals(int index) {
            for (int i = Math.min(index + 1, intervals.size() - 1), e = Math.max(index - 1, 0); i > e; i--) {
                Interval a = intervals.get(i - 1), b = intervals.get(i);
                if (a.end + 1 == b.start) {
                    a.end = b.end;
                    intervals.remove(i);
                }
            }
        }

        /**
         * @return index which is either a pointer to the interval containing v or a position at which v can be added
         */
        private int findInterval(long v) {
            int l = 0, p = 0, r = intervals.size();
            while (l < r) {
                p = (l + r) / 2;
                Interval i = intervals.get(p);
                if (i.end < v) {
                    l = p + 1;
                } else
                if (v < i.start) {
                    r = p;
                } else {
                    return p;
                }
            }
            if (!intervals.isEmpty() && intervals.get(p).end < v) {
                p++;
            }
            return p;
        }

        /**
         * Get the UUID for the server that generated the GTIDs.
         * @return the server's UUID; never null
         */
        public String getUUID() {
            return uuid;
        }

        /**
         * Get the intervals of transaction numbers.
         * @return the immutable transaction intervals; never null
         */
        public List<Interval> getIntervals() {
            return Collections.unmodifiableList(intervals);
        }

        /**
         * Determine if the set of transaction numbers from this server is completely within the set of transaction
         * numbers from the set of transaction numbers in the supplied set.
         * @param other the set to compare with this set
         * @return {@code true} if this server's transaction numbers are equal to or a subset of the transaction
         * numbers of the supplied set, or false otherwise
         */
        public boolean isContainedWithin(UUIDSet other) {
            if (other == null) {
                return false;
            }
            if (!this.getUUID().equalsIgnoreCase(other.getUUID())) {
                // not even the same server ...
                return false;
            }
            if (this.intervals.isEmpty()) {
                return true;
            }
            if (other.intervals.isEmpty()) {
                return false;
            }
            // every interval in this must be within an interval of the other ...
            for (Interval thisInterval : this.intervals) {
                boolean found = false;
                for (Interval otherInterval : other.intervals) {
                    if (thisInterval.isContainedWithin(otherInterval)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false; // didn't find a match
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            return uuid.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj instanceof UUIDSet) {
                UUIDSet that = (UUIDSet) obj;
                return this.getUUID().equalsIgnoreCase(that.getUUID()) &&
                        this.getIntervals().equals(that.getIntervals());
            }
            return super.equals(obj);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (sb.length() != 0) {
                sb.append(',');
            }
            sb.append(uuid).append(':');
            Iterator<Interval> iter = intervals.iterator();
            if (iter.hasNext()) {
                sb.append(iter.next());
            }
            while (iter.hasNext()) {
                sb.append(':');
                sb.append(iter.next());
            }
            return sb.toString();
        }
    }

    /**
     * An interval of contiguous transaction identifiers.
     * @see GtidSet
     */
    public static final class Interval implements Comparable<Interval> {

        private long start;
        private long end;

        public Interval(long start, long end) {
            this.start = start;
            this.end = end;
        }

        /**
         * Get the starting transaction number in this interval.
         * @return this interval's first transaction number
         */
        public long getStart() {
            return start;
        }

        /**
         * Get the ending transaction number in this interval.
         * @return this interval's last transaction number
         */
        public long getEnd() {
            return end;
        }

        /**
         * Determine if this interval is completely within the supplied interval.
         * @param other the interval to compare with
         * @return {@code true} if the {@link #getStart() start} is greater than or equal to the supplied interval's
         * {@link #getStart() start} and the {@link #getEnd() end} is less than or equal to the supplied
         * interval's {@link #getEnd() end}, or {@code false} otherwise
         */
        public boolean isContainedWithin(Interval other) {
            if (other == this) {
                return true;
            }
            if (other == null) {
                return false;
            }
            return this.getStart() >= other.getStart() && this.getEnd() <= other.getEnd();
        }

        @Override
        public int hashCode() {
            return (int) getStart();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof Interval) {
                Interval that = (Interval) obj;
                return this.getStart() == that.getStart() && this.getEnd() == that.getEnd();
            }
            return false;
        }

        @Override
        public String toString() {
            return start + "-" + end;
        }

        @Override
        public int compareTo(Interval o) {
            return saturatedCast(this.start - o.start);
        }

        private static int saturatedCast(long value) {
            if (value > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            if (value < Integer.MIN_VALUE) {
                return Integer.MIN_VALUE;
            }
            return (int) value;
        }
    }

}
