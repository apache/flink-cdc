/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.meta.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** A structure describes an offset in a redo log event. */
public class RedoLogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String REDO_LOG_SCN_OFFSET_KEY = "scn";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";

    public static final RedoLogOffset NO_STOPPING_OFFSET = new RedoLogOffset(Long.MIN_VALUE);
    public static final RedoLogOffset INITIAL_OFFSET = new RedoLogOffset(0);

    public RedoLogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public RedoLogOffset(String readUTF, long scn) {
        this.offset = new HashMap<>();
        this.offset.put(REDO_LOG_SCN_OFFSET_KEY, String.valueOf(scn));
    }

    public RedoLogOffset(long scn) {
        this.offset = new HashMap<>();
        this.offset.put(REDO_LOG_SCN_OFFSET_KEY, String.valueOf(scn));
    }

    public RedoLogOffset(String scn) {
        this.offset = new HashMap<>();
        this.offset.put(REDO_LOG_SCN_OFFSET_KEY, scn);
    }

    public Map<String, String> getOffset() {
        return offset;
    }

    public long longOffsetValue(Map<String, ?> values, String key) {
        Object obj = values.get(key);
        if (obj == null) {
            return 0L;
        }
        if (obj instanceof Number) {
            return ((Number) obj).longValue();
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (NumberFormatException e) {
            throw new ConnectException(
                    "Source offset '"
                            + key
                            + "' parameter value "
                            + obj
                            + " could not be converted to a long");
        }
    }

    @Override
    public int compareTo(Offset that) {
        if (Objects.isNull(this.offset.get(REDO_LOG_SCN_OFFSET_KEY))
                && Objects.isNull(that.getOffset().get(REDO_LOG_SCN_OFFSET_KEY))) {
            return 0;
        }
        if (Objects.isNull(this.offset.get(REDO_LOG_SCN_OFFSET_KEY))
                && !Objects.isNull(that.getOffset().get(REDO_LOG_SCN_OFFSET_KEY))) {
            return -1;
        }
        if (!Objects.isNull(this.offset.get(REDO_LOG_SCN_OFFSET_KEY))
                && Objects.isNull(that.getOffset().get(REDO_LOG_SCN_OFFSET_KEY))) {
            return 1;
        }

        Long thisScn = Long.parseLong(this.offset.get(REDO_LOG_SCN_OFFSET_KEY));
        Long thatScn = Long.parseLong(that.getOffset().get(REDO_LOG_SCN_OFFSET_KEY));
        return thisScn.compareTo(thatScn);
    }

    public boolean isAtOrBefore(RedoLogOffset that) {
        return this.compareTo(that) <= 0;
    }

    public boolean isBefore(RedoLogOffset that) {
        return this.compareTo(that) < 0;
    }

    public boolean isAtOrAfter(RedoLogOffset that) {
        return this.compareTo(that) >= 0;
    }

    public boolean isAfter(RedoLogOffset that) {
        return this.compareTo(that) > 0;
    }

    @Override
    public String toString() {
        return offset.toString();
    }
}
