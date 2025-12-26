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

package org.apache.flink.cdc.connectors.gaussdb.source.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** A utility class for converting GaussDB types to Flink types. */
public class GaussDBTypeUtils {

    private static final Pattern PRECISION_SCALE_PATTERN =
            Pattern.compile("\\((\\d+)(?:\\s*,\\s*(\\d+))?\\)");

    private static final int DEFAULT_TEMPORAL_PRECISION = 6;

    /**
     * Converts GaussDB type name string to corresponding Flink {@link DataType}.
     *
     * <p>Supports all types defined in PRD FR-5 and their aliases:
     *
     * <ul>
     *   <li>INTEGER / INT4 -&gt; INT
     *   <li>BIGINT / INT8 -&gt; BIGINT
     *   <li>SMALLINT / INT2 -&gt; SMALLINT
     *   <li>DECIMAL / NUMERIC -&gt; DECIMAL (keep precision/scale if present)
     *   <li>REAL / FLOAT4 -&gt; FLOAT
     *   <li>DOUBLE PRECISION / FLOAT8 -&gt; DOUBLE
     *   <li>VARCHAR / CHARACTER VARYING / TEXT -&gt; STRING
     *   <li>BOOLEAN -&gt; BOOLEAN
     *   <li>DATE -&gt; DATE
     *   <li>TIME -&gt; TIME
     *   <li>TIMESTAMP -&gt; TIMESTAMP
     *   <li>TIMESTAMPTZ -&gt; TIMESTAMP_LTZ
     *   <li>BYTEA -&gt; BYTES
     *   <li>JSON / JSONB -&gt; STRING
     * </ul>
     *
     * <p>Returns {@link DataTypes#NULL()} when the input type name is null/blank.
     */
    public static DataType convertGaussDBType(String gaussdbTypeName) {
        if (gaussdbTypeName == null || gaussdbTypeName.trim().isEmpty()) {
            return DataTypes.NULL();
        }

        final String normalized = normalizeTypeName(gaussdbTypeName);

        // DECIMAL / NUMERIC need to preserve precision/scale if present.
        if (startsWithOneOf(normalized, "decimal", "numeric")) {
            final int[] precisionScale = extractPrecisionScale(gaussdbTypeName);
            if (precisionScale != null) {
                return DataTypes.DECIMAL(precisionScale[0], precisionScale[1]);
            }
            // Fallback when precision/scale is not explicitly defined.
            return DataTypes.DECIMAL(DecimalType.MAX_PRECISION, 18);
        }

        final int temporalPrecision = extractTemporalPrecision(gaussdbTypeName);

        switch (normalized) {
            case "integer":
            case "int":
            case "int4":
            case "serial":
                return DataTypes.INT();
            case "bigint":
            case "int8":
            case "bigserial":
                return DataTypes.BIGINT();
            case "smallint":
            case "int2":
                return DataTypes.SMALLINT();
            case "real":
            case "float4":
                return DataTypes.FLOAT();
            case "double precision":
            case "float8":
            case "double":
                return DataTypes.DOUBLE();
            case "varchar":
            case "character varying":
            case "text":
            case "json":
            case "jsonb":
                return DataTypes.STRING();
            case "boolean":
            case "bool":
                return DataTypes.BOOLEAN();
            case "date":
                return DataTypes.DATE();
            case "time":
            case "time without time zone":
                return DataTypes.TIME(temporalPrecision);
            case "timestamp":
            case "timestamp without time zone":
                return DataTypes.TIMESTAMP(temporalPrecision);
            case "timestamptz":
            case "timestamp with time zone":
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(temporalPrecision);
            case "bytea":
                return DataTypes.BYTES();
            default:
                throw new UnsupportedOperationException(
                        String.format("Doesn't support GaussDB type '%s' yet", gaussdbTypeName));
        }
    }

    private static String normalizeTypeName(String gaussdbTypeName) {
        String s = gaussdbTypeName.trim();
        if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
            s = s.substring(1, s.length() - 1).trim();
        }
        // Strip schema prefix like "pg_catalog.int4" / "public.xxx"
        int lastDot = s.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < s.length() - 1) {
            s = s.substring(lastDot + 1);
        }

        // Remove size/precision/scale part but keep other qualifiers, e.g.:
        // "timestamp(3) with time zone" -> "timestamp with time zone".
        s = s.replaceAll("\\(\\s*\\d+(?:\\s*,\\s*\\d+)?\\s*\\)", "");

        // Normalize whitespace and case.
        s = s.replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);

        // Normalize common verbose timestamp/time spellings.
        if (s.startsWith("timestamp with time zone")) {
            return "timestamp with time zone";
        }
        if (s.startsWith("timestamp without time zone")) {
            return "timestamp without time zone";
        }
        if (s.startsWith("time without time zone")) {
            return "time without time zone";
        }

        return s;
    }

    private static boolean startsWithOneOf(String s, String... prefixes) {
        for (String prefix : prefixes) {
            if (s.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /** Returns (precision, scale) or null if not explicitly defined. */
    private static int[] extractPrecisionScale(String gaussdbTypeName) {
        Matcher matcher = PRECISION_SCALE_PATTERN.matcher(gaussdbTypeName);
        if (!matcher.find()) {
            return null;
        }
        int precision = Integer.parseInt(matcher.group(1));
        int scale = matcher.group(2) == null ? 0 : Integer.parseInt(matcher.group(2));
        return new int[] {precision, scale};
    }

    private static int extractTemporalPrecision(String gaussdbTypeName) {
        Matcher matcher = PRECISION_SCALE_PATTERN.matcher(gaussdbTypeName);
        if (!matcher.find()) {
            return DEFAULT_TEMPORAL_PRECISION;
        }
        int precision = Integer.parseInt(matcher.group(1));
        // TIME(p) / TIMESTAMP(p) use a single precision.
        if (matcher.group(2) != null) {
            return DEFAULT_TEMPORAL_PRECISION;
        }
        return precision;
    }
}
