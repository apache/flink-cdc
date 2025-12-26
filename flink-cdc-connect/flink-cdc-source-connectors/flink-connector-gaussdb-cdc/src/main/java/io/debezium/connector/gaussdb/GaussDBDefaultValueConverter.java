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

package io.debezium.connector.gaussdb;

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * A {@link DefaultValueConverter} implementation for GaussDB that handles function-based default
 * values.
 *
 * <p>GaussDB uses function calls like {@code CURRENT_TIMESTAMP}, {@code pg_systimestamp()}, {@code
 * nextval()}, etc. as default values. These cannot be converted to actual Java values during schema
 * building, so this converter returns {@link Optional#empty()} for such expressions.
 */
public class GaussDBDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOG = LoggerFactory.getLogger(GaussDBDefaultValueConverter.class);

    /**
     * Pattern to detect function-based default values. Matches expressions containing: - Function
     * calls with parentheses: nextval(), CURRENT_TIMESTAMP(), pg_systimestamp() - SQL keywords:
     * CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, now() - Sequence operations: nextval, currval
     * - GaussDB-specific functions: pg_systimestamp, systimestamp
     */
    private static final Pattern FUNCTION_PATTERN =
            Pattern.compile(
                    ".*\\(.*\\)|CURRENT_TIMESTAMP|CURRENT_DATE|CURRENT_TIME|"
                            + "nextval|currval|now\\(\\)|pg_systimestamp|systimestamp|"
                            + "LOCALTIMESTAMP|LOCALTIME",
                    Pattern.CASE_INSENSITIVE);

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        if (defaultValueExpression == null || defaultValueExpression.trim().isEmpty()) {
            return Optional.empty();
        }

        // Check if the default value is a function call or SQL keyword
        if (isFunctionBasedDefault(defaultValueExpression)) {
            LOG.debug(
                    "Skipping function-based default value '{}' for column '{}'",
                    defaultValueExpression,
                    column.name());
            return Optional.empty();
        }

        // For simple literal values, try to parse them
        // This handles cases like numeric literals, string literals, boolean values, etc.
        try {
            return parseLiteralValue(column, defaultValueExpression);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to parse default value '{}' for column '{}': {}. Skipping default value.",
                    defaultValueExpression,
                    column.name(),
                    e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Checks if the default value expression is a function call or SQL keyword that cannot be
     * converted to a literal value.
     */
    private boolean isFunctionBasedDefault(String expression) {
        String trimmed = expression.trim();
        return FUNCTION_PATTERN.matcher(trimmed).matches();
    }

    /**
     * Attempts to parse a literal default value based on the column type.
     *
     * <p>This is a simplified implementation that handles common cases. For complex type
     * conversions, it returns empty to avoid errors.
     */
    private Optional<Object> parseLiteralValue(Column column, String value) {
        String trimmed = value.trim();

        // Remove surrounding quotes for string literals
        if ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\""))) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }

        // Handle NULL
        if ("NULL".equalsIgnoreCase(trimmed)) {
            return Optional.empty();
        }

        // Handle boolean values
        if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
            return Optional.of(Boolean.parseBoolean(trimmed));
        }

        // For numeric types, try to parse as number
        switch (column.jdbcType()) {
            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                try {
                    return Optional.of(Integer.parseInt(trimmed));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            case java.sql.Types.BIGINT:
                try {
                    return Optional.of(Long.parseLong(trimmed));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
                try {
                    return Optional.of(Float.parseFloat(trimmed));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            case java.sql.Types.DOUBLE:
                try {
                    return Optional.of(Double.parseDouble(trimmed));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                try {
                    return Optional.of(new java.math.BigDecimal(trimmed));
                } catch (NumberFormatException e) {
                    return Optional.empty();
                }
            default:
                // For other types (strings, dates, etc.), return the trimmed string
                // The actual conversion will be handled by the value converter
                return Optional.of(trimmed);
        }
    }
}
