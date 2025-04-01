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

package com.github.shyiko.mysql.binlog.event.deserialization.json;

import com.github.shyiko.mysql.binlog.event.deserialization.ColumnType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig.useLegacyJsonFormat;

/**
 * Copied from mysql-binlog-connector-java 0.27.2 to add whitespace before value and after comma.
 *
 * <p>Line 105: Added whitespace before value, Line 213: Added whitespace after comma
 */
public class JsonStringFormatter implements JsonFormatter {

    /**
     * Value used for lookup tables to indicate that matching characters do not need to be escaped.
     */
    private static final int ESCAPE_NONE = 0;

    /**
     * Value used for lookup tables to indicate that matching characters are to be escaped using
     * standard escaping; for JSON this means (for example) using "backslash - u" escape method.
     */
    private static final int ESCAPE_GENERIC = -1;

    /**
     * A lookup table that determines which of the first 128 Unicode code points (single-byte UTF-8
     * characters) must be escaped. A value of '0' means no escaping is required; positive values
     * must be escaped with a preceding backslash; and negative values that generic escaping (e.g.,
     */
    private static final int[] ESCAPES;

    static {
        int[] escape = new int[128];
        // Generic escape for control characters ...
        for (int i = 0; i < 32; ++i) {
            escape[i] = ESCAPE_GENERIC;
        }
        // Backslash escape for other specific characters ...
        escape['"'] = '"';
        escape['\\'] = '\\';
        // Escaping of slash is optional, so let's not add it
        escape[0x08] = 'b';
        escape[0x09] = 't';
        escape[0x0C] = 'f';
        escape[0x0A] = 'n';
        escape[0x0D] = 'r';
        ESCAPES = escape;
    }

    private static final char[] HEX_CODES = "0123456789ABCDEF".toCharArray();

    private final StringBuilder sb = new StringBuilder();

    @Override
    public String toString() {
        return getString();
    }

    public String getString() {
        return sb.toString();
    }

    @Override
    public void beginObject(int numElements) {
        sb.append('{');
    }

    @Override
    public void beginArray(int numElements) {
        sb.append('[');
    }

    @Override
    public void endObject() {
        sb.append('}');
    }

    @Override
    public void endArray() {
        sb.append(']');
    }

    @Override
    public void name(String name) {
        sb.append('"');
        appendString(name);
        if (useLegacyJsonFormat) {
            sb.append("\":");
        } else {
            sb.append("\": ");
        }
    }

    @Override
    public void value(String value) {
        sb.append('"');
        appendString(value);
        sb.append('"');
    }

    @Override
    public void value(int value) {
        sb.append(Integer.toString(value));
    }

    @Override
    public void value(long value) {
        sb.append(Long.toString(value));
    }

    @Override
    public void value(double value) {
        // Double's toString method will result in scientific notation and loss of precision
        String str = Double.toString(value);
        if (str.contains("E")) {
            value(new BigDecimal(value));
        } else {
            sb.append(str);
        }
    }

    @Override
    public void value(BigInteger value) {
        // Using the BigInteger.toString() method will result in scientific notation, so instead ...
        value(new BigDecimal(value));
    }

    @Override
    public void value(BigDecimal value) {
        // Using the BigInteger.toString() method will result in scientific notation, so instead ...
        sb.append(value.toPlainString());
    }

    @Override
    public void value(boolean value) {
        sb.append(Boolean.toString(value));
    }

    @Override
    public void valueNull() {
        sb.append("null");
    }

    @Override
    public void valueYear(int year) {
        sb.append(year);
    }

    @Override
    public void valueDate(int year, int month, int day) {
        sb.append('"');
        appendDate(year, month, day);
        sb.append('"');
    }

    @Override
    // checkstyle, please ignore ParameterNumber for the next line
    public void valueDatetime(
            int year, int month, int day, int hour, int min, int sec, int microSeconds) {
        sb.append('"');
        appendDate(year, month, day);
        sb.append(' ');
        appendTime(hour, min, sec, microSeconds);
        sb.append('"');
    }

    @Override
    public void valueTime(int hour, int min, int sec, int microSeconds) {
        sb.append('"');
        if (hour < 0) {
            sb.append('-');
            hour = Math.abs(hour);
        }
        appendTime(hour, min, sec, microSeconds);
        sb.append('"');
    }

    @Override
    public void valueTimestamp(long secondsPastEpoch, int microSeconds) {
        sb.append(secondsPastEpoch);
        appendSixDigitUnsignedInt(microSeconds, false);
    }

    @Override
    public void valueOpaque(ColumnType type, byte[] value) {
        sb.append('"');
        sb.append(Base64.getEncoder().encodeToString(value));
        sb.append('"');
    }

    @Override
    public void nextEntry() {
        if (useLegacyJsonFormat) {
            sb.append(",");
        } else {
            sb.append(", ");
        }
    }

    /**
     * Append a string by escaping any characters that must be escaped.
     *
     * @param original the string to be written; may not be null
     */
    protected void appendString(String original) {
        for (int i = 0, len = original.length(); i < len; ++i) {
            char c = original.charAt(i);
            int ch = c;
            if (ch < 0 || ch >= ESCAPES.length || ESCAPES[ch] == 0) {
                sb.append(c);
                continue;
            }
            int escape = ESCAPES[ch];
            if (escape > 0) { // 2-char escape, fine
                sb.append('\\');
                sb.append((char) escape);
            } else {
                unicodeEscape(ch);
            }
        }
    }

    /**
     * Append a generic Unicode escape for given character.
     *
     * @param charToEscape the character to escape
     */
    private void unicodeEscape(int charToEscape) {
        sb.append('\\');
        sb.append('u');
        if (charToEscape > 0xFF) {
            int hi = (charToEscape >> 8) & 0xFF;
            sb.append(HEX_CODES[hi >> 4]);
            sb.append(HEX_CODES[hi & 0xF]);
            charToEscape &= 0xFF;
        } else {
            sb.append('0');
            sb.append('0');
        }
        // We know it's a control char, so only the last 2 chars are non-0
        sb.append(HEX_CODES[charToEscape >> 4]);
        sb.append(HEX_CODES[charToEscape & 0xF]);
    }

    protected void appendTwoDigitUnsignedInt(int value) {
        assert value >= 0;
        assert value < 100;
        if (value < 10) {
            sb.append("0").append(value);
        } else {
            sb.append(value);
        }
    }

    protected void appendFourDigitUnsignedInt(int value) {
        if (value < 10) {
            sb.append("000").append(value);
        } else if (value < 100) {
            sb.append("00").append(value);
        } else if (value < 1000) {
            sb.append("0").append(value);
        } else {
            sb.append(value);
        }
    }

    protected void appendSixDigitUnsignedInt(int value, boolean trimTrailingZeros) {
        assert value > 0;
        assert value < 1000000;
        // Add prefixes if necessary ...
        if (value < 10) {
            sb.append("00000");
        } else if (value < 100) {
            sb.append("0000");
        } else if (value < 1000) {
            sb.append("000");
        } else if (value < 10000) {
            sb.append("00");
        } else if (value < 100000) {
            sb.append("0");
        }
        if (trimTrailingZeros) {
            // Remove any trailing 0's ...
            for (int i = 0; i != 6; ++i) {
                if (value % 10 == 0) {
                    value /= 10;
                }
            }
            sb.append(value);
        }
    }

    protected void appendDate(int year, int month, int day) {
        if (year < 0) {
            sb.append('-');
            year = Math.abs(year);
        }
        appendFourDigitUnsignedInt(year);
        sb.append('-');
        appendTwoDigitUnsignedInt(month);
        sb.append('-');
        appendTwoDigitUnsignedInt(day);
    }

    protected void appendTime(int hour, int min, int sec, int microSeconds) {
        appendTwoDigitUnsignedInt(hour);
        sb.append(':');
        appendTwoDigitUnsignedInt(min);
        sb.append(':');
        appendTwoDigitUnsignedInt(sec);
        if (microSeconds != 0) {
            sb.append('.');
            appendSixDigitUnsignedInt(microSeconds, true);
        }
    }
}
