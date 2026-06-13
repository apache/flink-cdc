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

package org.apache.flink.cdc.connectors.lancedb.utils;

import java.util.regex.Pattern;

/** Lance dataset name helpers. */
public class LanceDbNameUtils {

    private static final Pattern SIMPLE_NAME_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_NAME_LENGTH = 255;

    private LanceDbNameUtils() {}

    public static String validateSimpleName(String name, String role) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException(role + " must not be blank.");
        }
        String trimmed = name.trim();
        if (trimmed.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                    role + " " + trimmed + " exceeds length limit " + MAX_NAME_LENGTH + ".");
        }
        if (!SIMPLE_NAME_PATTERN.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(
                    role + " " + trimmed + " must match [A-Za-z_][A-Za-z0-9_]*.");
        }
        return trimmed;
    }

    public static String normalizeSimpleName(String raw, String role) {
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException(role + " must not be blank.");
        }
        String normalized = raw.trim().replaceAll("[^A-Za-z0-9_]", "_");
        if (normalized.isEmpty()) {
            normalized = "_";
        }
        if (!Character.isLetter(normalized.charAt(0)) && normalized.charAt(0) != '_') {
            normalized = "_" + normalized;
        }
        if (normalized.length() > MAX_NAME_LENGTH) {
            normalized = normalized.substring(0, MAX_NAME_LENGTH);
        }
        return validateSimpleName(normalized, role);
    }
}
