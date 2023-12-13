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

package org.apache.flink.cdc.connectors.db2.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import io.debezium.connector.db2.Lsn;
import io.debezium.connector.db2.SourceInfo;

import java.util.HashMap;
import java.util.Map;

/** A structure describes an offset in a Lsn event. */
public class LsnOffset extends Offset {

    public static final LsnOffset INITIAL_OFFSET =
            new LsnOffset(Lsn.valueOf(new byte[] {Byte.MIN_VALUE}));
    public static final LsnOffset NO_STOPPING_OFFSET =
            new LsnOffset(Lsn.valueOf(new byte[] {Byte.MAX_VALUE}));
    private static final String EVENT_SERIAL_NO_KEY = "event_serial_no";

    public LsnOffset(Lsn changeLsn, Lsn commitLsn, Long eventSerialNo) {
        Map<String, String> offsetMap = new HashMap<>();

        if (changeLsn != null && changeLsn.isAvailable()) {
            offsetMap.put(SourceInfo.CHANGE_LSN_KEY, changeLsn.toString());
        }
        if (commitLsn != null && commitLsn.isAvailable()) {
            offsetMap.put(SourceInfo.COMMIT_LSN_KEY, commitLsn.toString());
        }
        if (eventSerialNo != null) {
            offsetMap.put(EVENT_SERIAL_NO_KEY, String.valueOf(eventSerialNo));
        }

        this.offset = offsetMap;
    }

    public LsnOffset(Lsn changeLsn) {
        this(changeLsn, null, null);
    }

    @Override
    public int compareTo(Offset offset) {
        LsnOffset that = (LsnOffset) offset;
        // the NO_STOPPING_OFFSET is the max offset
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Lsn lsn = Lsn.valueOf(this.offset.get(SourceInfo.COMMIT_LSN_KEY));
        Lsn targetLsn = Lsn.valueOf(that.offset.get(SourceInfo.COMMIT_LSN_KEY));
        if (targetLsn.isAvailable()) {
            if (lsn.isAvailable()) {
                return lsn.compareTo(targetLsn);
            }
            return -1;
        } else if (lsn.isAvailable()) {
            return 1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LsnOffset)) {
            return false;
        }
        LsnOffset that = (LsnOffset) o;
        return offset.equals(that.offset);
    }
}
