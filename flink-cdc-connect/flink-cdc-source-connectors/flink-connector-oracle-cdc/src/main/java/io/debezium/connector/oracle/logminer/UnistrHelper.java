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

package io.debezium.connector.oracle.logminer;

import java.util.ArrayList;
import java.util.List;

/** A utility/helper class to support decoding Oracle Unicode String function values, {@code UNISTR}. */
public class UnistrHelper {

    private static final String UNITSTR_FUNCTION_START = "UNISTR('";
    private static final String UNISTR_FUNCTION_END = "')";

    public static boolean isUnistrFunction(String data) {
        return data != null
                && data.startsWith(UNITSTR_FUNCTION_START)
                && data.endsWith(UNISTR_FUNCTION_END);
    }

    public static String convert(String data) {
        if (data == null || data.length() == 0) {
            return data;
        }

        // Multiple UNISTR function calls maybe concatenated together using "||".
        // Only split on SQL concatenation operators outside UNISTR quoted data.
        final List<String> parts = tokenize(data);

        final StringBuilder result = new StringBuilder();
        for (String part : parts) {
            final String trimmed = part.trim();
            if (isUnistrFunction(trimmed)) {
                result.append(
                        decode(
                                trimmed.substring(
                                        UNITSTR_FUNCTION_START.length(), trimmed.length() - 2)));
            } else {
                result.append(data);
            }
        }
        return result.toString();
    }

    private static String decode(String value) {
        final StringBuilder result = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && value.length() >= i + 4) {
                result.append(
                        Character.toChars(Integer.parseInt(value.substring(i + 1, i + 5), 16)));
                i += 4;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static List<String> tokenize(String data) {
        final List<String> parts = new ArrayList<>();
        final int length = data.length();

        boolean inQuotedData = false;
        int startIndex = 0;

        for (int i = 0; i < length; i++) {
            if (stringMatches(data, i, UNITSTR_FUNCTION_START)) {
                inQuotedData = true;
                if (startIndex == -1) {
                    startIndex = i;
                }
            } else if (inQuotedData && stringMatches(data, i, UNISTR_FUNCTION_END)) {
                inQuotedData = false;
            } else if (!inQuotedData && stringMatches(data, i, "||")) {
                parts.add(data.substring(startIndex, i).trim());

                i += 1;
                startIndex = i + 1;
            }
        }

        if (startIndex < data.length()) {
            parts.add(data.substring(startIndex).trim());
        }

        return parts;
    }

    private static boolean stringMatches(String str, int index, String token) {
        return str.regionMatches(index, token, 0, token.length());
    }
}
