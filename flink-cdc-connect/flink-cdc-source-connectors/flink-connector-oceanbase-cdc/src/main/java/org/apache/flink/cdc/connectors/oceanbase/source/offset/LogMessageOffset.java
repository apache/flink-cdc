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

package org.apache.flink.cdc.connectors.oceanbase.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** A structure describes a fine-grained offset of {@link LogMessage}. */
public class LogMessageOffset extends Offset {

    private static final Logger LOG = LoggerFactory.getLogger(LogMessageOffset.class);

    public static final String TIMESTAMP_KEY = "timestamp";
    public static final String COMMIT_VERSION_KEY = "commit_version";
    public static final String TRANSACTIONS_TO_SKIP_KEY = "trans";
    public static final String EVENTS_TO_SKIP_KEY = "events";

    public static final LogMessageOffset INITIAL_OFFSET = new LogMessageOffset(Long.MIN_VALUE);
    public static final LogMessageOffset NO_STOPPING_OFFSET = new LogMessageOffset(Long.MAX_VALUE);

    public LogMessageOffset(Map<String, ?> offset) {
        Map<String, String> offsetMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetMap.put(
                    entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
        }
        this.offset = offsetMap;
    }

    public static LogMessageOffset from(LogMessage message) {
        DataMessage.Record.Type type = message.getOpt();
        if (type == DataMessage.Record.Type.BEGIN
                || type == DataMessage.Record.Type.DDL
                || type == DataMessage.Record.Type.HEARTBEAT) {
            return new LogMessageOffset(message.getTimestamp(), getCommitVersion(message), 0, 0);
        }
        return new LogMessageOffset(message.getTimestamp(), null, 0, 0);
    }

    private static String getCommitVersion(LogMessage message) {
        String microseconds;
        try {
            microseconds = message.getTimestampUsec();
        } catch (IOException e) {
            LOG.warn("Failed to read timestamp in microseconds from log message", e);
            microseconds = null;
        }
        return (microseconds == null || "0".equals(microseconds))
                ? null
                : message.getTimestamp() + microseconds;
    }

    public LogMessageOffset(long timestamp) {
        this(Long.toString(timestamp), null, 0, 0);
    }

    public LogMessageOffset(
            @Nonnull String timestamp, String commitVersion, long transToSkip, long eventsToSkip) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_KEY, timestamp);
        offsetMap.put(TRANSACTIONS_TO_SKIP_KEY, String.valueOf(transToSkip));
        offsetMap.put(EVENTS_TO_SKIP_KEY, String.valueOf(eventsToSkip));
        if (commitVersion != null) {
            offsetMap.put(COMMIT_VERSION_KEY, commitVersion);
        }
        this.offset = offsetMap;
    }

    public String getTimestamp() {
        return offset.get(TIMESTAMP_KEY);
    }

    public String getCommitVersion() {
        return offset.get(COMMIT_VERSION_KEY);
    }

    public long getTransactionsToSkip() {
        return longOffsetValue(offset, TRANSACTIONS_TO_SKIP_KEY);
    }

    public long getEventsToSkip() {
        return longOffsetValue(offset, EVENTS_TO_SKIP_KEY);
    }

    @Override
    public int compareTo(@Nonnull Offset offset) {
        LogMessageOffset that = (LogMessageOffset) offset;

        int flag;
        flag = compareLong(getTimestamp(), that.getTimestamp());
        if (flag != 0) {
            return flag;
        }
        flag = compareLong(getCommitVersion(), that.getCommitVersion());
        if (flag != 0) {
            return flag;
        }
        flag = Long.compare(getTransactionsToSkip(), that.getTransactionsToSkip());
        if (flag != 0) {
            return flag;
        }
        return Long.compare(getEventsToSkip(), that.getEventsToSkip());
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
}
