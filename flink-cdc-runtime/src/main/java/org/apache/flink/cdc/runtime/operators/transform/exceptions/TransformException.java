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

package org.apache.flink.cdc.runtime.operators.transform.exceptions;

import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataExtractor;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** A generic {@link RuntimeException} thrown during transform failure. */
public class TransformException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final String UNKNOWN_PLACEHOLDER = "(Unknown)";

    public TransformException(
            String command,
            Event event,
            @Nullable TableId tableId,
            @Nullable Schema schemaBefore,
            @Nullable Schema schemaAfter,
            Throwable cause) {
        super(
                String.format(
                        "Failed to %s with\n"
                                + "\t%s\n"
                                + "for table\n"
                                + "\t%s\n"
                                + "from schema\n"
                                + "\t%s\n"
                                + "to schema\n"
                                + "\t%s.",
                        command,
                        prettyPrintEvent(event, schemaBefore),
                        Optional.ofNullable(tableId)
                                .map(Object::toString)
                                .orElse(UNKNOWN_PLACEHOLDER),
                        Optional.ofNullable(schemaBefore)
                                .map(Object::toString)
                                .orElse(UNKNOWN_PLACEHOLDER),
                        Optional.ofNullable(schemaAfter)
                                .map(Object::toString)
                                .orElse(UNKNOWN_PLACEHOLDER)),
                cause);
    }

    @VisibleForTesting
    public static String prettyPrintEvent(Event event, @Nullable Schema schema) {
        try {
            if (event instanceof DataChangeEvent && schema != null) {
                return ((DataChangeEvent) event)
                        .toReadableString(
                                rec -> BinaryRecordDataExtractor.extractRecord(rec, schema));
            } else {
                return event.toString();
            }
        } catch (Exception e) {
            // Do not throw an exception if deserialization fails since we're already in an
            // unexpected state.
            return event.toString();
        }
    }

    /** Prints an incrementally-sorted column name map like {@code $0 -> id, $1 -> name, ...}. */
    public static String prettyPrintColumnNameMap(Map<String, String> columnNameMap) {
        return columnNameMap.entrySet().stream()
                .sorted(
                        Comparator.comparingInt(
                                entry -> Integer.parseInt(entry.getValue().substring(1))))
                .map(entry -> String.format("%s -> %s", entry.getValue(), entry.getKey()))
                .collect(Collectors.joining(", "));
    }
}
