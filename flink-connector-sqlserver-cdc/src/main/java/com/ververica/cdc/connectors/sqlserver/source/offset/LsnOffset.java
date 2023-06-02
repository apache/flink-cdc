/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.sqlserver.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.HashMap;
import java.util.Map;

/** A structure describes an offset in a Lsn event. */
public class LsnOffset extends Offset {

    public static final LsnOffset INITIAL_OFFSET =
            new LsnOffset(Lsn.valueOf(new byte[] {Byte.MIN_VALUE}));
    public static final LsnOffset NO_STOPPING_OFFSET =
            new LsnOffset(Lsn.valueOf(new byte[] {Byte.MAX_VALUE}));

    public LsnOffset(Lsn scn, Lsn commitScn, Long eventSerialNo) {
        Map<String, String> offsetMap = new HashMap<>();

        if (scn != null && scn.isAvailable()) {
            offsetMap.put(SourceInfo.CHANGE_LSN_KEY, scn.toString());
        }
        if (commitScn != null && commitScn.isAvailable()) {
            offsetMap.put(SourceInfo.COMMIT_LSN_KEY, commitScn.toString());
        }
        if (eventSerialNo != null) {
            offsetMap.put(SourceInfo.EVENT_SERIAL_NO_KEY, String.valueOf(eventSerialNo));
        }

        this.offset = offsetMap;
    }

    public LsnOffset(Lsn lsn) {
        this(lsn, null, null);
    }

    public Lsn getLcn() {
        return Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
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

        Lsn lsn = this.getLcn();
        Lsn targetLsn = that.getLcn();
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
