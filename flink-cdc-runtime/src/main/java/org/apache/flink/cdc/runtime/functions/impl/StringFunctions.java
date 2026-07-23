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

package org.apache.flink.cdc.runtime.functions.impl;

import org.apache.flink.cdc.common.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.cdc.common.types.variant.Variant;

import org.apache.calcite.runtime.SqlFunctions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** String built-in functions. */
public class StringFunctions {

    private static final Logger LOG = LoggerFactory.getLogger(StringFunctions.class);

    public static int charLength(String str) {
        return str.length();
    }

    public static String trim(String symbol, String target, String str) {
        return str.trim();
    }

    /**
     * Returns a string resulting from replacing all substrings that match the regular expression
     * with replacement.
     */
    public static String regexpReplace(String str, String regex, String replacement) {
        if (str == null || regex == null || replacement == null) {
            return null;
        }
        try {
            return str.replaceAll(regex, Matcher.quoteReplacement(replacement));
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Exception in regexpReplace('%s', '%s', '%s')",
                            str, regex, replacement),
                    e);
            // return null if exception in regex replace
            return null;
        }
    }

    public static String concat(String... str) {
        return String.join("", str);
    }

    public static String concatWs(String separator, String... str) {
        if (separator == null || str == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String s : str) {
            if (s == null) {
                continue;
            }
            if (!first) {
                builder.append(separator);
            }
            builder.append(s);
            first = false;
        }
        return builder.toString();
    }

    public static boolean like(String str, String regex) {
        return Pattern.compile(regex).matcher(str).find();
    }

    public static Boolean like(String str, String pattern, String escape) {
        if (str == null || pattern == null || escape == null) {
            return null;
        }
        return SqlFunctions.like(str, pattern, escape);
    }

    public static boolean notLike(String str, String regex) {
        return !like(str, regex);
    }

    public static Boolean notLike(String str, String pattern, String escape) {
        return LogicalFunctions.not(like(str, pattern, escape));
    }

    public static Boolean similarTo(String str, String pattern) {
        if (str == null || pattern == null) {
            return null;
        }
        return SqlFunctions.similar(str, pattern);
    }

    public static Boolean similarTo(String str, String pattern, String escape) {
        if (str == null || pattern == null || escape == null) {
            return null;
        }
        return SqlFunctions.similar(str, pattern, escape);
    }

    public static Boolean notSimilarTo(String str, String pattern) {
        return LogicalFunctions.not(similarTo(str, pattern));
    }

    public static Boolean notSimilarTo(String str, String pattern, String escape) {
        return LogicalFunctions.not(similarTo(str, pattern, escape));
    }

    public static String substr(String str, int beginIndex) {
        return substring(str, beginIndex);
    }

    public static String substr(String str, int beginIndex, int length) {
        return substring(str, beginIndex, length);
    }

    public static String substring(String str, int beginIndex) {
        return substring(str, beginIndex, Integer.MAX_VALUE);
    }

    public static String substring(String str, int beginIndex, int length) {
        if (length < 0) {
            LOG.error(
                    "length of 'substring(str, beginIndex, length)' must be >= 0 and Int type, but length = {}",
                    length);
            throw new RuntimeException(
                    "length of 'substring(str, beginIndex, length)' must be >= 0 and Int type, but length = "
                            + length);
        }
        if (length > Integer.MAX_VALUE || beginIndex > Integer.MAX_VALUE) {
            LOG.error(
                    "length or start of 'substring(str, beginIndex, length)' must be Int type, but length = {}, beginIndex = {}",
                    beginIndex,
                    length);
            throw new RuntimeException(
                    "length or start of 'substring(str, beginIndex, length)' must be Int type, but length = "
                            + beginIndex
                            + ", beginIndex = "
                            + length);
        }
        if (str.isEmpty()) {
            return "";
        }

        int startPos;
        int endPos;

        if (beginIndex > 0) {
            startPos = beginIndex - 1;
            if (startPos >= str.length()) {
                return "";
            }
        } else if (beginIndex < 0) {
            startPos = str.length() + beginIndex;
            if (startPos < 0) {
                return "";
            }
        } else {
            startPos = 0;
        }

        if ((str.length() - startPos) < length) {
            endPos = str.length();
        } else {
            endPos = startPos + length;
        }
        return str.substring(startPos, endPos);
    }

    public static String upper(String str) {
        return str.toUpperCase();
    }

    public static String lower(String str) {
        return str.toLowerCase();
    }

    public static String overlay(String str, String replacement, Number start) {
        if (replacement == null) {
            return null;
        }
        return overlay(str, replacement, start, replacement.length());
    }

    public static String overlay(String str, String replacement, Number start, Number length) {
        if (str == null || replacement == null || start == null || length == null) {
            return null;
        }
        int startPosition = start.intValue();
        int len = length.intValue();
        if (startPosition <= 0 || startPosition > str.length()) {
            return str;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(str, 0, startPosition - 1);
        builder.append(replacement);
        if (startPosition + len <= str.length() && len > 0) {
            builder.append(str.substring(startPosition - 1 + len));
        }
        return builder.toString();
    }

    public static Integer position(String seek, String str) {
        return position(seek, str, 1);
    }

    public static Integer position(String seek, String str, Number from) {
        if (seek == null || str == null || from == null) {
            return null;
        }
        if (seek.isEmpty()) {
            return 1;
        }
        int fromCodePoint = Math.max(from.intValue() - 1, 0);
        int codePointLength = str.codePointCount(0, str.length());
        if (fromCodePoint > codePointLength) {
            return 0;
        }
        int fromIndex = str.offsetByCodePoints(0, fromCodePoint);
        int index = str.indexOf(seek, fromIndex);
        return index < 0 ? 0 : str.codePointCount(0, index) + 1;
    }

    public static Integer instr(String str, String subString) {
        if (str == null || subString == null) {
            return null;
        }
        int index = str.indexOf(subString);
        return index < 0 ? 0 : str.codePointCount(0, index) + 1;
    }

    public static Integer locate(String seek, String str) {
        return position(seek, str);
    }

    public static Integer locate(String seek, String str, Number from) {
        return position(seek, str, from);
    }

    public static String ltrim(String str) {
        return ltrim(str, " ");
    }

    public static String ltrim(String str, String trimStr) {
        return trim(str, trimStr, true, false);
    }

    public static String rtrim(String str) {
        return rtrim(str, " ");
    }

    public static String rtrim(String str, String trimStr) {
        return trim(str, trimStr, false, true);
    }

    public static String btrim(String str) {
        return btrim(str, " ");
    }

    public static String btrim(String str, String trimStr) {
        return trim(str, trimStr, true, true);
    }

    public static String lpad(String base, Number len, String pad) {
        return pad(base, len, pad, true);
    }

    public static String rpad(String base, Number len, String pad) {
        return pad(base, len, pad, false);
    }

    public static String replace(String str, String oldStr, String replacement) {
        if (str == null || oldStr == null || replacement == null) {
            return null;
        }
        return str.replace(oldStr, replacement);
    }

    public static String repeat(String str, Number repeat) {
        if (str == null || repeat == null) {
            return null;
        }
        int count = repeat.intValue();
        if (count <= 0) {
            return "";
        }
        return str.repeat(count);
    }

    public static String left(String str, Number length) {
        if (str == null || length == null) {
            return null;
        }
        int len = length.intValue();
        if (len <= 0) {
            return "";
        }
        int codePointLength = str.codePointCount(0, str.length());
        if (len >= codePointLength) {
            return str;
        }
        return str.substring(0, str.offsetByCodePoints(0, len));
    }

    public static String right(String str, Number length) {
        if (str == null || length == null) {
            return null;
        }
        int len = length.intValue();
        if (len <= 0) {
            return "";
        }
        int codePointLength = str.codePointCount(0, str.length());
        if (len >= codePointLength) {
            return str;
        }
        return str.substring(str.offsetByCodePoints(0, codePointLength - len));
    }

    public static Boolean startswith(String str, String prefix) {
        if (str == null || prefix == null) {
            return null;
        }
        return str.startsWith(prefix);
    }

    public static Boolean startswith(byte[] bytes, byte[] prefix) {
        if (bytes == null || prefix == null) {
            return null;
        }
        return matchesAt(bytes, prefix, 0);
    }

    public static Boolean endswith(String str, String suffix) {
        if (str == null || suffix == null) {
            return null;
        }
        return str.endsWith(suffix);
    }

    public static Boolean endswith(byte[] bytes, byte[] suffix) {
        if (bytes == null || suffix == null) {
            return null;
        }
        return matchesAt(bytes, suffix, bytes.length - suffix.length);
    }

    public static String toBase64(String str) {
        if (str == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
    }

    public static String fromBase64(String str) {
        if (str == null) {
            return null;
        }
        return new String(
                Base64.getDecoder().decode(str.getBytes(StandardCharsets.UTF_8)),
                StandardCharsets.UTF_8);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String uuid(byte[] b) {
        return UUID.nameUUIDFromBytes(b).toString();
    }

    public static Variant tryParseJson(String jsonStr) {
        return tryParseJson(jsonStr, false);
    }

    public static Variant tryParseJson(String jsonStr, boolean allowDuplicateKeys) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return null;
        }

        try {
            return BinaryVariantInternalBuilder.parseJson(jsonStr, allowDuplicateKeys);
        } catch (Throwable e) {
            return null;
        }
    }

    public static Variant parseJson(String jsonStr) {
        return parseJson(jsonStr, false);
    }

    public static Variant parseJson(String jsonStr, boolean allowDuplicateKeys) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return null;
        }

        try {
            return BinaryVariantInternalBuilder.parseJson(jsonStr, allowDuplicateKeys);
        } catch (Throwable e) {
            throw new IllegalArgumentException(
                    String.format("Failed to parse json string: %s", jsonStr), e);
        }
    }

    private static String trim(
            String str, String trimStr, boolean trimLeading, boolean trimTrailing) {
        if (str == null || trimStr == null) {
            return null;
        }
        if (trimStr.isEmpty()) {
            return str;
        }
        int begin = 0;
        int end = str.length();
        while (trimLeading && begin < end) {
            int codePoint = str.codePointAt(begin);
            if (trimStr.indexOf(codePoint) < 0) {
                break;
            }
            begin += Character.charCount(codePoint);
        }
        while (trimTrailing && begin < end) {
            int codePoint = str.codePointBefore(end);
            if (trimStr.indexOf(codePoint) < 0) {
                break;
            }
            end -= Character.charCount(codePoint);
        }
        return str.substring(begin, end);
    }

    private static boolean matchesAt(byte[] bytes, byte[] target, int offset) {
        if (offset < 0 || offset + target.length > bytes.length) {
            return false;
        }
        for (int i = 0; i < target.length; i++) {
            if (bytes[offset + i] != target[i]) {
                return false;
            }
        }
        return true;
    }

    private static String pad(String base, Number length, String pad, boolean leftPad) {
        if (base == null || length == null || pad == null) {
            return null;
        }
        int len = length.intValue();
        if (len < 0 || pad.isEmpty()) {
            return null;
        } else if (len == 0) {
            return "";
        }

        char[] data = new char[len];
        char[] baseChars = base.toCharArray();
        char[] padChars = pad.toCharArray();

        if (leftPad) {
            int pos = Math.max(len - base.length(), 0);
            for (int i = 0; i < pos; i += pad.length()) {
                for (int j = 0; j < pad.length() && j < pos - i; j++) {
                    data[i + j] = padChars[j];
                }
            }
            int i = 0;
            while (pos + i < len && i < base.length()) {
                data[pos + i] = baseChars[i];
                i++;
            }
        } else {
            int pos = 0;
            while (pos < base.length() && pos < len) {
                data[pos] = baseChars[pos];
                pos++;
            }
            while (pos < len) {
                int i = 0;
                while (i < pad.length() && i < len - pos) {
                    data[pos + i] = padChars[i];
                    i++;
                }
                pos += pad.length();
            }
        }
        return new String(data);
    }
}
