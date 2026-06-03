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

package org.apache.flink.cdc.connectors.milvus.utils;

import org.apache.flink.cdc.common.event.TableId;

import java.util.Map;
import java.util.regex.Pattern;

/** Naming helpers for Milvus collections, fields, and indexes. */
public class MilvusNameUtils {

    private static final int MAX_IDENTIFIER_LENGTH = 255;
    private static final Pattern VALID_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,254}");

    private MilvusNameUtils() {}

    public static String resolveCollectionName(
            TableId tableId, Map<TableId, String> mappings, boolean normalizeEnabled) {
        String mapped = mappings.get(tableId);
        if (mapped != null && !mapped.trim().isEmpty()) {
            return validateIdentifier(mapped.trim(), "collection");
        }
        String raw = tableId.identifier();
        return normalizeEnabled ? normalizeIdentifier(raw) : validateIdentifier(raw, "collection");
    }

    public static String normalizeIdentifier(String value) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c >= 'A' && c <= 'Z')
                    || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9')
                    || c == '_') {
                builder.append(c);
            } else {
                builder.append('_');
            }
        }
        if (builder.length() == 0) {
            builder.append('_');
        }
        char first = builder.charAt(0);
        if (!((first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z') || first == '_')) {
            builder.insert(0, '_');
        }
        if (builder.length() > MAX_IDENTIFIER_LENGTH) {
            builder.setLength(MAX_IDENTIFIER_LENGTH);
        }
        return builder.toString();
    }

    public static String normalizePartitionName(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return validateIdentifier(normalizeIdentifier(value.trim()), "partition");
    }

    public static String validateIdentifier(String value, String kind) {
        if (!VALID_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "Invalid Milvus "
                            + kind
                            + " name '"
                            + value
                            + "'. Names must start with a letter or underscore and contain only letters, digits, and underscores.");
        }
        return value;
    }

    public static String indexName(String vectorFieldName) {
        return normalizeIdentifier(vectorFieldName + "_idx");
    }
}
