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

package org.apache.flink.cdc.connectors.gaussdb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

import io.debezium.connector.gaussdb.connection.Lsn;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/** The offset for GaussDB. */
public class GaussDBOffset extends Offset {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBOffset.class);

    public static final String LSN_KEY = "lsn";

    public static final GaussDBOffset INITIAL_OFFSET = new GaussDBOffset(Lsn.INVALID_LSN);
    public static final GaussDBOffset NO_STOPPING_OFFSET = new GaussDBOffset(Lsn.NO_STOPPING_LSN);

    // used by GaussDBOffsetFactory
    GaussDBOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public GaussDBOffset(Lsn lsn) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(LSN_KEY, formatLsn(lsn == null ? Lsn.INVALID_LSN : lsn));
        this.offset = offsetMap;
    }

    public static GaussDBOffset of(SourceRecord dataRecord) {
        return of(dataRecord.sourceOffset());
    }

    public static GaussDBOffset of(Map<String, ?> offsetMap) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (LSN_KEY.equals(key)) {
                offsetStrMap.put(key, serializeLsn(value));
            } else {
                offsetStrMap.put(key, value == null ? null : value.toString());
            }
        }
        return new GaussDBOffset(offsetStrMap);
    }

    public Lsn getLsn() {
        if (this.offset == null) {
            return Lsn.INVALID_LSN;
        }
        return parseLsn(this.offset.get(LSN_KEY));
    }

    public String getLsnString() {
        return formatLsn(getLsn());
    }

    @Override
    public int compareTo(Offset o) {
        GaussDBOffset that = (GaussDBOffset) o;

        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Lsn lsn = this.getLsn();
        Lsn targetLsn = that.getLsn();
        LOG.debug("comparing {} and {}", lsn, targetLsn);
        if (targetLsn.isValid()) {
            if (lsn.isValid()) {
                return lsn.compareTo(targetLsn);
            }
            return -1;
        } else if (lsn.isValid()) {
            return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "Offset{lsn=" + getLsnString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GaussDBOffset)) {
            return false;
        }
        GaussDBOffset that = (GaussDBOffset) o;
        return offset.equals(that.offset);
    }

    private static String serializeLsn(@Nullable Object lsnValue) {
        return formatLsn(parseLsn(lsnValue));
    }

    private static Lsn parseLsn(@Nullable Object lsnValue) {
        if (lsnValue == null) {
            return Lsn.INVALID_LSN;
        }
        if (lsnValue instanceof Lsn) {
            return (Lsn) lsnValue;
        }
        if (lsnValue instanceof Number) {
            return Lsn.valueOf(((Number) lsnValue).longValue());
        }

        String strValue = lsnValue.toString().trim();
        if (strValue.isEmpty()) {
            return Lsn.INVALID_LSN;
        }
        if (strValue.contains("/")) {
            return Lsn.valueOf(strValue);
        }

        try {
            return Lsn.valueOf(Long.parseLong(strValue));
        } catch (NumberFormatException e) {
            return Lsn.valueOf(Long.parseUnsignedLong(strValue));
        }
    }

    /**
     * Formats the LSN string as {@code <log>/<segment>} (upper-hex, segment always 8 digits).
     *
     * <p>Examples: {@code 0/00000000}, {@code 16/3002D50}, {@code FFFFFFFF/FFFFFFFF}.
     */
    private static String formatLsn(Lsn lsn) {
        final ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(lsn.asLong());
        ((java.nio.Buffer) buf).position(0);
        final int logicalXlog = buf.getInt();
        final int segment = buf.getInt();
        return String.format("%X/%08X", logicalXlog, segment);
    }
}
