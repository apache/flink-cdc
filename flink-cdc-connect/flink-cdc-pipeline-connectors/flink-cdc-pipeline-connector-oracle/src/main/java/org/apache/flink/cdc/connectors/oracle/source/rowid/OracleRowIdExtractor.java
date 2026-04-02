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

package org.apache.flink.cdc.connectors.oracle.source.rowid;

import io.debezium.data.Envelope;
import oracle.sql.ROWID;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Optional;

/** Utility methods for extracting Oracle ROWID values from Debezium records. */
public class OracleRowIdExtractor {

    private static final Logger LOG = LoggerFactory.getLogger(OracleRowIdExtractor.class);
    private static final String ROWID_FIELD = ROWID.class.getSimpleName();

    private OracleRowIdExtractor() {}

    /**
     * Extracts the Oracle {@link ROWID} from a Debezium data change record.
     *
     * <p>The ROWID is read from the payload first, using {@code after} and then {@code before},
     * matching Debezium's Oracle envelope structure.
     */
    public static Optional<ROWID> extractRowId(SourceRecord record) {
        if (record == null || record.value() == null) {
            return Optional.empty();
        }

        try {
            Struct value = (Struct) record.value();

            Optional<String> rowIdString =
                    extractFromStruct(getStructIfPresent(value, Envelope.FieldName.AFTER));
            if (!rowIdString.isPresent()) {
                rowIdString =
                        extractFromStruct(getStructIfPresent(value, Envelope.FieldName.BEFORE));
            }
            if (!rowIdString.isPresent()) {
                rowIdString = extractFromHeaders(record);
            }

            if (!rowIdString.isPresent()) {
                return Optional.empty();
            }

            return Optional.of(new ROWID(rowIdString.get()));
        } catch (ClassCastException | DataException | SQLException e) {
            LOG.debug("Failed to extract ROWID from record.", e);
            return Optional.empty();
        }
    }

    private static Struct getStructIfPresent(Struct value, String fieldName) {
        if (value.schema() == null || value.schema().field(fieldName) == null) {
            return null;
        }
        return value.getStruct(fieldName);
    }

    private static Optional<String> extractFromStruct(Struct struct) {
        if (struct == null
                || struct.schema() == null
                || struct.schema().field(ROWID_FIELD) == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(struct.getString(ROWID_FIELD));
    }

    private static Optional<String> extractFromHeaders(SourceRecord record) {
        if (!(record.headers() instanceof ConnectHeaders)) {
            return Optional.empty();
        }

        ConnectHeaders headers = (ConnectHeaders) record.headers();
        for (Header header : headers) {
            if (ROWID_FIELD.equals(header.key()) && header.value() != null) {
                return Optional.of(header.value().toString());
            }
        }
        return Optional.empty();
    }
}
