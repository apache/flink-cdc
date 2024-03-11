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

package org.apache.flink.cdc.connectors.oceanbase.source.converter;

import org.apache.flink.cdc.connectors.oceanbase.source.config.OceanBaseConnectorConfig;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Bits;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

/** JdbcValueConverters for OceanBase. */
public class OceanBaseValueConverters extends JdbcValueConverters {

    public static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    public static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yy hh.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .appendPattern(" a")
                    .toFormatter(Locale.ENGLISH);

    private final String compatibleMode;
    private final String serverTimezone;

    public OceanBaseValueConverters(OceanBaseConnectorConfig connectorConfig) {
        super(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                x -> x,
                BigIntUnsignedMode.PRECISE,
                connectorConfig.binaryHandlingMode());
        this.compatibleMode = connectorConfig.getCompatibleMode();
        this.serverTimezone = connectorConfig.getServerTimeZone();
    }

    @Override
    protected int getTimePrecision(Column column) {
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            return super.getTimePrecision(column);
        }
        return column.scale().orElse(0);
    }

    protected boolean isUnsignedColumn(Column column) {
        return column.typeName().toUpperCase().contains("UNSIGNED");
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug(
                "Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
            case Types.BIT:
                if (column.length() > 1) {
                    return Bits.builder(column.length());
                }
                return SchemaBuilder.bool();
            case Types.TINYINT:
                if (column.length() == 1) {
                    return SchemaBuilder.bool();
                }
                if (isUnsignedColumn(column)) {
                    return SchemaBuilder.int16();
                }
                return SchemaBuilder.int8();
            case Types.SMALLINT:
                if (isUnsignedColumn(column)) {
                    return SchemaBuilder.int32();
                }
                return SchemaBuilder.int16();
            case Types.INTEGER:
                if (!column.typeName().toUpperCase().startsWith("MEDIUMINT")
                        && isUnsignedColumn(column)) {
                    return SchemaBuilder.int64();
                }
                return SchemaBuilder.int32();
            case Types.BIGINT:
                if (isUnsignedColumn(column)) {
                    return Decimal.builder(0);
                }
                return SchemaBuilder.int64();
            case Types.FLOAT:
                return getDecimalSchema(column);
            case Types.NUMERIC:
            case Types.DECIMAL:
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    return getDecimalSchema(column);
                }
                return getNumericSchema(column);
            case Types.REAL:
                return SchemaBuilder.float32();
            case Types.DOUBLE:
                return SchemaBuilder.float64();
            case Types.DATE:
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    if (column.typeName().equalsIgnoreCase("YEAR")) {
                        return io.debezium.time.Year.builder();
                    }
                    if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                        return io.debezium.time.Date.builder();
                    }
                    return org.apache.kafka.connect.data.Date.builder();
                }
                return getTimestampSchema(column);
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return io.debezium.time.MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return io.debezium.time.Time.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return io.debezium.time.MicroTime.builder();
                    }
                    return io.debezium.time.NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                return getTimestampSchema(column);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return SchemaBuilder.string();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return binaryMode.getSchema();
            default:
                return super.schemaBuilder(column);
        }
    }

    protected SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            int scale = column.scale().get();
            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return SchemaBuilder.int8();
                } else if (width < 5) {
                    return SchemaBuilder.int16();
                } else if (width < 10) {
                    return SchemaBuilder.int32();
                } else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }
        }
        return getDecimalSchema(column);
    }

    protected SchemaBuilder getDecimalSchema(Column column) {
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(0));
    }

    protected SchemaBuilder getTimestampSchema(Column column) {
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return io.debezium.time.Timestamp.builder();
            }
            if (getTimePrecision(column) <= 6) {
                return MicroTimestamp.builder();
            }
            return NanoTimestamp.builder();
        }
        return org.apache.kafka.connect.data.Timestamp.builder();
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.BIT:
                return convertBits(column, fieldDefn);
            case Types.TINYINT:
                if (column.length() == 1) {
                    return data -> convertBit(column, fieldDefn, data);
                }
                if (isUnsignedColumn(column)) {
                    return data -> convertSmallInt(column, fieldDefn, data);
                }
                return data -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                if (isUnsignedColumn(column)) {
                    return data -> convertInteger(column, fieldDefn, data);
                }
                return data -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                if (column.typeName().toUpperCase().startsWith("MEDIUMINT")) {
                    return data -> convertInteger(column, fieldDefn, data);
                }
                if (isUnsignedColumn(column)) {
                    return data -> convertBigInt(column, fieldDefn, data);
                }
                return data -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                if (isUnsignedColumn(column)) {
                    switch (bigIntUnsignedMode) {
                        case LONG:
                            return (data) -> convertBigInt(column, fieldDefn, data);
                        case PRECISE:
                            return (data) -> convertUnsignedBigint(column, fieldDefn, data);
                    }
                }
                return (data) -> convertBigInt(column, fieldDefn, data);
            case Types.FLOAT:
                return data -> convertDecimal(column, fieldDefn, data);
            case Types.NUMERIC:
            case Types.DECIMAL:
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    return data -> convertDecimal(column, fieldDefn, data);
                }
                return data -> convertNumeric(column, fieldDefn, data);
            case Types.REAL:
                return data -> convertReal(column, fieldDefn, data);
            case Types.DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.DATE:
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    if (column.typeName().equalsIgnoreCase("YEAR")) {
                        return (data) -> convertYearToInt(column, fieldDefn, data);
                    }
                    if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                        return (data) -> convertDateToEpochDays(column, fieldDefn, data);
                    }
                    return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data);
                }
                return (data) -> convertTimestamp(column, fieldDefn, data);
            case Types.TIME:
                return (data) -> convertTime(column, fieldDefn, data);
            case Types.TIMESTAMP:
                return data -> convertTimestamp(column, fieldDefn, data);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return data -> convertString(column, fieldDefn, data);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return (data) -> convertBinary(column, fieldDefn, data, binaryMode);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data instanceof String) {
            return ByteBuffer.allocate(numBytes).putLong(Long.parseLong((String) data)).array();
        }
        return super.convertBits(column, fieldDefn, data, numBytes);
    }

    @Override
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Boolean.parseBoolean((String) data) || "1".equals(data);
        }
        return super.convertBit(column, fieldDefn, data);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Byte.parseByte((String) data);
        }
        if (data instanceof Number) {
            return ((Number) data).byteValue();
        }
        throw new IllegalArgumentException(
                "Unexpected value for JDBC type "
                        + column.jdbcType()
                        + " and column "
                        + column
                        + ": class="
                        + data.getClass());
    }

    @Override
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return new BigInteger((String) data).longValue();
        }
        return super.convertBigInt(column, fieldDefn, data);
    }

    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return new BigDecimal((String) data);
        }
        if (data instanceof BigInteger) {
            return new BigDecimal((BigInteger) data);
        }
        return convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Float.parseFloat((String) data);
        }
        return super.convertReal(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Double.parseDouble((String) data);
        }
        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        if (column.scale().isPresent()) {
            int scale = column.scale().get();

            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return convertTinyInt(column, fieldDefn, data);
                } else if (width < 5) {
                    return convertSmallInt(column, fieldDefn, data);
                } else if (width < 10) {
                    return convertInteger(column, fieldDefn, data);
                } else if (width < 19) {
                    return convertBigInt(column, fieldDefn, data);
                }
            }
        }
        return convertDecimal(column, fieldDefn, data);
    }

    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof Date) {
            return ((Date) data).toLocalDate().getYear();
        }
        return convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Date.valueOf((String) data);
        }
        return super.convertDateToEpochDays(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Date.valueOf((String) data);
        }
        return super.convertDateToEpochDaysAsDate(column, fieldDefn, data);
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Time.valueOf((String) data);
        }
        return super.convertTime(column, fieldDefn, data);
    }

    protected Object convertTimestamp(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            if ("mysql".equalsIgnoreCase(compatibleMode)) {
                data = Timestamp.valueOf(((String) data).trim());
            } else {
                data = resolveTimestampStringAsInstant((String) data);
            }
        }
        if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return convertTimestampToEpochMillis(column, fieldDefn, data);
            }
            if (getTimePrecision(column) <= 6) {
                return convertTimestampToEpochMicros(column, fieldDefn, data);
            }
            return convertTimestampToEpochNanos(column, fieldDefn, data);
        }
        return convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
    }

    protected Instant resolveTimestampStringAsInstant(String dateText) {
        LocalDateTime dateTime;
        if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
            dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
        } else {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
        }
        return dateTime.atZone(ZoneId.of(serverTimezone)).toInstant();
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            try {
                Clob clob = (Clob) data;
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }
        if (data instanceof String) {
            String s = (String) data;
            if (EMPTY_CLOB_FUNCTION.equals(s)) {
                return column.isOptional() ? null : "";
            }
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(
            Column column,
            Field fieldDefn,
            Object data,
            CommonConnectorConfig.BinaryHandlingMode mode) {
        try {
            if (data instanceof Blob) {
                Blob blob = (Blob) data;
                data = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
            }
            if (data instanceof String) {
                String str = (String) data;
                if (EMPTY_BLOB_FUNCTION.equals(str)) {
                    data = column.isOptional() ? null : "";
                }
            }
            return super.convertBinary(column, fieldDefn, data, mode);
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
        }
    }
}
