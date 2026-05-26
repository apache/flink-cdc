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

package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** A utility class for processing {@link SourceRecord}s from a Postgres source. */
public final class PostgresSourceRecordUtils {

    private PostgresSourceRecordUtils() {}

    /**
     * Checks whether a {@link SourceRecord} is a logical message.
     *
     * @param record the source record
     * @return true if the record is a logical message, false otherwise
     */
    public static boolean isLogicalMessage(SourceRecord record) {
        if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            return struct.schema().field(Envelope.FieldName.OPERATION) != null
                    && "m".equals(struct.getString(Envelope.FieldName.OPERATION));
        }
        return false;
    }

    /**
     * Returns the prefix of a logical message.
     *
     * @param record the source record
     * @return the prefix of the logical message
     */
    public static String getLogicalMessagePrefix(SourceRecord record) {
        if (record.value() instanceof Struct) {
            Struct struct = (Struct) record.value();
            return struct.getString("message_prefix");
        }
        return null;
    }
}
