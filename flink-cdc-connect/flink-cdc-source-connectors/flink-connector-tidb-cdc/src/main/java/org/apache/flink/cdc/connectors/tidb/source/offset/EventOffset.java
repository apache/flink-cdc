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

package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import org.tikv.common.meta.TiTimestamp;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EventOffset extends Offset {
    public static final String TIMESTAMP_KEY = "timestamp";
    // TimeStamp Oracle from pd
    public static final String COMMIT_VERSION_KEY = "commit_version";

    public static final EventOffset INITIAL_OFFSET =
            new EventOffset(Collections.singletonMap(TIMESTAMP_KEY, "0"));
    public static final EventOffset NO_STOPPING_OFFSET = new EventOffset(Long.MAX_VALUE);

    public EventOffset(Map<String, ?> offset) {
        Map<String, String> offsetMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        this.offset = offsetMap;
    }

    public EventOffset(@Nonnull String timestamp, String commitVersion) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_KEY, timestamp);
        if (commitVersion != null) {
            offsetMap.put(COMMIT_VERSION_KEY, commitVersion);
        }
        this.offset = offsetMap;
    }

    public EventOffset(long binlogEpochMill) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochMill));
        offsetMap.put(
                COMMIT_VERSION_KEY,
                String.valueOf(new TiTimestamp(binlogEpochMill, 0).getVersion()));
        this.offset = offsetMap;
    }

    public String getTimestamp() {
        return offset.get(TIMESTAMP_KEY);
    }

    public String getCommitVersion() {
        if (offset.get(COMMIT_VERSION_KEY) == null) {
            String timestamp = getTimestamp();
            // timestamp to commit version.
            return String.valueOf(new TiTimestamp(Long.parseLong(timestamp), 0).getVersion());
        }
        return offset.get(COMMIT_VERSION_KEY);
    }

    @Override
    public int compareTo(@Nonnull Offset o) {
        EventOffset that = (EventOffset) o;

        int flag;
        flag = compareLong(getTimestamp(), that.getTimestamp());
        if (flag != 0) {
            return flag;
        }
        return compareLong(getCommitVersion(), that.getCommitVersion());
    }

    private int compareLong(String a, String b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        return Long.compare(Long.parseLong(a), Long.parseLong(b));
    }

    public static EventOffset of(Map<String, ?> offsetMap) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            offsetStrMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new EventOffset(offsetStrMap);
    }

    public static long getStartTs(Offset offset) {
        if (offset.getOffset().get(COMMIT_VERSION_KEY) != null) {
            return Long.parseLong(offset.getOffset().get(COMMIT_VERSION_KEY));
        } else {
            return new TiTimestamp(Long.parseLong(offset.getOffset().get(TIMESTAMP_KEY)), 0)
                    .getVersion();
        }
    }
}
