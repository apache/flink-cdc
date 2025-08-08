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

import java.util.Properties;

/** Utility class for handling Debezium configuration parameters. */
public class DebeziumConfigUtils {

    /** Decimal handling modes from Debezium configuration. */
    public enum DecimalHandlingMode {
        PRECISE,
        STRING,
        DOUBLE
    }

    private static final String DECIMAL_HANDLING_MODE_KEY = "decimal.handling.mode";
    private static final DecimalHandlingMode DEFAULT_DECIMAL_HANDLING_MODE =
            DecimalHandlingMode.PRECISE;

    /** Extract decimal handling mode from Debezium properties. */
    public static DecimalHandlingMode getDecimalHandlingMode(Properties debeziumProperties) {
        if (debeziumProperties == null) {
            return DEFAULT_DECIMAL_HANDLING_MODE;
        }

        String mode = debeziumProperties.getProperty(DECIMAL_HANDLING_MODE_KEY);
        if (mode == null) {
            return DEFAULT_DECIMAL_HANDLING_MODE;
        }

        switch (mode.toLowerCase()) {
            case "string":
                return DecimalHandlingMode.STRING;
            case "double":
                return DecimalHandlingMode.DOUBLE;
            case "precise":
            default:
                return DecimalHandlingMode.PRECISE;
        }
    }

    /**
     * Handle numeric type conversion based on precision, scale and decimal handling mode. This
     * method provides consistent numeric type handling for source connectors (table API).
     */
    public static org.apache.flink.table.types.DataType handleNumericTypeForTable(
            int precision, int scale, DecimalHandlingMode decimalMode) {
        // Handle based on decimal handling mode
        switch (decimalMode) {
            case STRING:
                // Always return STRING when mode is string
                return org.apache.flink.table.api.DataTypes.STRING();
            case DOUBLE:
                // Always return DOUBLE when mode is double
                return org.apache.flink.table.api.DataTypes.DOUBLE();
            case PRECISE:
            default:
                // For precise mode, use DECIMAL if precision is valid, otherwise BIGINT
                if (precision > 0 && precision <= 38) {
                    return org.apache.flink.table.api.DataTypes.DECIMAL(precision, scale);
                } else {
                    // For invalid precision (like PostgreSQL numeric without explicit precision),
                    // fall back to BIGINT to avoid serialization issues
                    return org.apache.flink.table.api.DataTypes.BIGINT();
                }
        }
    }

    /** Handle numeric type conversion for common DataTypes (used by pipeline connectors). */
    public static org.apache.flink.cdc.common.types.DataType handleNumericTypeForCommon(
            int precision, int scale, DecimalHandlingMode decimalMode) {
        // Handle based on decimal handling mode
        switch (decimalMode) {
            case STRING:
                // Always return STRING when mode is string
                return org.apache.flink.cdc.common.types.DataTypes.STRING();
            case DOUBLE:
                // Always return DOUBLE when mode is double
                return org.apache.flink.cdc.common.types.DataTypes.DOUBLE();
            case PRECISE:
            default:
                // For precise mode, use DECIMAL if precision is valid, otherwise BIGINT
                if (precision > 0 && precision <= 38) {
                    return org.apache.flink.cdc.common.types.DataTypes.DECIMAL(precision, scale);
                } else {
                    // For invalid precision (like PostgreSQL numeric without explicit precision),
                    // fall back to BIGINT to avoid serialization issues
                    return org.apache.flink.cdc.common.types.DataTypes.BIGINT();
                }
        }
    }

    private DebeziumConfigUtils() {
        // Utility class
    }
}
