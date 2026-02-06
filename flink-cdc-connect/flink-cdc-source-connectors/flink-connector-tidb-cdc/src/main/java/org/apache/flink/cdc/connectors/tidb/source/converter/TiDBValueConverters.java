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

package org.apache.flink.cdc.connectors.tidb.source.converter;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.util.FlinkRuntimeException;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;
import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlGeometry;
import io.debezium.connector.mysql.MySqlUnsignedIntegerConverter;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.data.Json;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Year;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.util.List;
import java.util.regex.Pattern;

/** JdbcValueConverters for tiDB. */
public class TiDBValueConverters extends JdbcValueConverters {

    /** Handler for parsing errors. */
    @FunctionalInterface
    public interface ParsingErrorHandler {
        void error(String message, Exception exception);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDBValueConverters.class);
    /** Used to parse values of TIME columns. Format: 000:00:00.000000. */
    private static final Pattern TIME_FIELD_PATTERN =
            Pattern.compile("(\\-?[0-9]*):([0-9]*):([0-9]*)(\\.([0-9]*))?");

    /** Used to parse values of DATE columns. Format: 000-00-00. */
    private static final Pattern DATE_FIELD_PATTERN = Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*)");

    /** Used to parse values of TIMESTAMP columns. Format: 000-00-00 00:00:00.000. */
    private static final Pattern TIMESTAMP_FIELD_PATTERN =
            Pattern.compile("([0-9]*)-([0-9]*)-([0-9]*) .*");

    public static Temporal adjustTemporal(Temporal temporal) {
        if (temporal.isSupported(ChronoField.YEAR)) {
            int year = temporal.get(ChronoField.YEAR);
            if (0 <= year && year <= 69) {
                temporal = temporal.plus(2000, ChronoUnit.YEARS);
            } else if (70 <= year && year <= 99) {
                temporal = temporal.plus(1900, ChronoUnit.YEARS);
            }
        }
        return temporal;
    }

    // todo
    public TiDBValueConverters(TiDBConnectorConfig connectorConfig) {
        super(
                connectorConfig.getDecimalMode(),
                connectorConfig.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                x -> x,
                BigIntUnsignedMode.PRECISE,
                connectorConfig.binaryHandlingMode());
    }

    public TiDBValueConverters(
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            BigIntUnsignedMode bigIntUnsignedMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode) {
        this(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryMode,
                x -> x,
                TiDBValueConverters::defaultParsingErrorHandler);
    }

    public TiDBValueConverters(
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            BigIntUnsignedMode bigIntUnsignedMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode,
            TemporalAdjuster adjuster,
            ParsingErrorHandler parsingErrorHandler) {
        super(
                decimalMode,
                temporalPrecisionMode,
                ZoneOffset.UTC,
                adjuster,
                bigIntUnsignedMode,
                binaryMode);
        //        this.parsingErrorHandler = parsingErrorHandler;
    }

    @Override
    protected ByteOrder byteOrderOfBitType() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog
        // client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return Json.builder();
        }
        if (matches(typeName, "POINT")) {
            return Point.builder();
        }
        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return Geometry.builder();
        }
        if (matches(typeName, "YEAR")) {
            return Year.builder();
        }
        if (matches(typeName, "ENUM")) {
            String commaSeparatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.Enum.builder(commaSeparatedOptions);
        }
        if (matches(typeName, "SET")) {
            String commaSeparatedOptions = extractEnumAndSetOptionsAsString(column);
            return io.debezium.data.EnumSet.builder(commaSeparatedOptions);
        }
        if (matches(typeName, "SMALLINT UNSIGNED")
                || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT2 UNSIGNED")
                || matches(typeName, "INT2 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned SMALLINT 16-bit data source, INT32 will be required to
            // safely capture all valid values
            // Source:
            // https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int32();
        }
        if (matches(typeName, "INT UNSIGNED")
                || matches(typeName, "INT UNSIGNED ZEROFILL")
                || matches(typeName, "INT4 UNSIGNED")
                || matches(typeName, "INT4 UNSIGNED ZEROFILL")) {
            // In order to capture unsigned INT 32-bit data source, INT64 will be required to safely
            // capture all valid values
            // Source:
            // https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
            return SchemaBuilder.int64();
        }
        if (matches(typeName, "BIGINT UNSIGNED")
                || matches(typeName, "BIGINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT8 UNSIGNED")
                || matches(typeName, "INT8 UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return SchemaBuilder.int64();
                case PRECISE:
                    // In order to capture unsigned INT 64-bit data source,
                    // org.apache.kafka.connect.data.Decimal:Byte will be required to safely capture
                    // all valid values with scale of 0
                    // Source:
                    // https://kafka.apache.org/0102/javadoc/org/apache/kafka/connect/data/Schema.Type.html
                    return Decimal.builder(0);
            }
        }

        // Otherwise, let the base class handle it ...
        return super.schemaBuilder(column);
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        // Handle a few MySQL-specific types based upon how they are handled by the MySQL binlog
        // client ...
        String typeName = column.typeName().toUpperCase();
        if (matches(typeName, "JSON")) {
            return (data) -> convertJson(column, fieldDefn, data);
        }
        if (matches(typeName, "GEOMETRY")
                || matches(typeName, "LINESTRING")
                || matches(typeName, "POLYGON")
                || matches(typeName, "MULTIPOINT")
                || matches(typeName, "MULTILINESTRING")
                || matches(typeName, "MULTIPOLYGON")
                || isGeometryCollection(typeName)) {
            return (data -> convertGeometry(column, fieldDefn, data));
        }
        if (matches(typeName, "POINT")) {
            // backwards compatibility
            return (data -> convertPoint(column, fieldDefn, data));
        }
        if (matches(typeName, "YEAR")) {
            return (data) -> convertYearToInt(column, fieldDefn, data);
        }
        if (matches(typeName, "ENUM")) {
            // Build up the character array based upon the column's type ...
            List<String> options = extractEnumAndSetOptions(column);
            return (data) -> convertEnumToString(options, column, fieldDefn, data);
        }
        if (matches(typeName, "SET")) {
            // Build up the character array based upon the column's type ...
            List<String> options = extractEnumAndSetOptions(column);
            return (data) -> convertSetToString(options, column, fieldDefn, data);
        }
        if (matches(typeName, "TINYINT UNSIGNED")
                || matches(typeName, "TINYINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT1 UNSIGNED")
                || matches(typeName, "INT1 UNSIGNED ZEROFILL")) {
            // Convert TINYINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary
            // settings
            return (data) -> convertUnsignedTinyint(column, fieldDefn, data);
        }
        if (matches(typeName, "SMALLINT UNSIGNED")
                || matches(typeName, "SMALLINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT2 UNSIGNED")
                || matches(typeName, "INT2 UNSIGNED ZEROFILL")) {
            // Convert SMALLINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary
            // settings
            return (data) -> convertUnsignedSmallint(column, fieldDefn, data);
        }
        if (matches(typeName, "MEDIUMINT UNSIGNED")
                || matches(typeName, "MEDIUMINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT3 UNSIGNED")
                || matches(typeName, "INT3 UNSIGNED ZEROFILL")
                || matches(typeName, "MIDDLEINT UNSIGNED")
                || matches(typeName, "MIDDLEINT UNSIGNED ZEROFILL")) {
            // Convert MEDIUMINT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary
            // settings
            return (data) -> convertUnsignedMediumint(column, fieldDefn, data);
        }
        if (matches(typeName, "INT UNSIGNED")
                || matches(typeName, "INT UNSIGNED ZEROFILL")
                || matches(typeName, "INT4 UNSIGNED")
                || matches(typeName, "INT4 UNSIGNED ZEROFILL")) {
            // Convert INT UNSIGNED internally from SIGNED to UNSIGNED based on the boundary
            // settings
            return (data) -> convertUnsignedInt(column, fieldDefn, data);
        }
        if (matches(typeName, "BIGINT UNSIGNED")
                || matches(typeName, "BIGINT UNSIGNED ZEROFILL")
                || matches(typeName, "INT8 UNSIGNED")
                || matches(typeName, "INT8 UNSIGNED ZEROFILL")) {
            switch (super.bigIntUnsignedMode) {
                case LONG:
                    return (data) -> convertBigInt(column, fieldDefn, data);
                case PRECISE:
                    // Convert BIGINT UNSIGNED internally from SIGNED to UNSIGNED based on the
                    // boundary settings
                    return (data) -> convertUnsignedBigint(column, fieldDefn, data);
            }
        }

        // We have to convert bytes encoded in the column's character set ...
        switch (column.jdbcType()) {
            case Types.CHAR: // variable-length
            case Types.VARCHAR: // variable-length
            case Types.LONGVARCHAR: // variable-length
            case Types.CLOB: // variable-length
            case Types.NCHAR: // fixed-length
            case Types.NVARCHAR: // fixed-length
            case Types.LONGNVARCHAR: // fixed-length
            case Types.NCLOB: // fixed-length
            case Types.DATALINK:
            case Types.SQLXML:
                Charset charset = charsetFor(column);
                if (charset != null) {
                    logger.debug("Using {} charset by default for column: {}", charset, column);
                    return (data) -> convertString(column, fieldDefn, charset, data);
                }
                logger.warn(
                        "Using UTF-8 charset by default for column without charset: {}", column);
                return (data) -> convertString(column, fieldDefn, StandardCharsets.UTF_8, data);
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return (data) -> convertTime(column, fieldDefn, data);
                }
                break;
            case Types.TIMESTAMP:
                return ((ValueConverter)
                                (data -> convertTimestampToLocalDateTime(column, fieldDefn, data)))
                        .and(super.converter(column, fieldDefn));
            default:
                break;
        }

        // Otherwise, let the base class handle it ...
        return super.converter(column, fieldDefn);
    }

    protected Object convertJson(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                "{}",
                (r) -> {
                    if (data instanceof byte[]) {
                        if (((byte[]) data).length == 0) {
                            r.deliver(column.isOptional() ? null : "{}");
                        } else {
                            try {
                                r.deliver(JsonBinary.parseAsString((byte[]) data));
                            } catch (IOException var5) {
                                throw new FlinkRuntimeException("tidbvalueConverters error");
                                //                                this.parsingErrorHandler.error(
                                //                                        "Failed to parse and read
                                // a JSON value on '"
                                //                                                + column
                                //                                                + "' value "
                                //                                                +
                                // Arrays.toString((byte[]) data),
                                //                                        var5);
                                //                                r.deliver(column.isOptional() ?
                                // null : "{}");
                            }
                        }
                    } else if (data instanceof String) {
                        r.deliver(data);
                    }
                });
    }

    protected Object convertPoint(Column column, Field fieldDefn, Object data) {
        MySqlGeometry empty = MySqlGeometry.createEmpty();
        return this.convertValue(
                column,
                fieldDefn,
                data,
                Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()),
                (r) -> {
                    if (data instanceof byte[]) {
                        MySqlGeometry mySqlGeometry = MySqlGeometry.fromBytes((byte[]) data);
                        if (!mySqlGeometry.isPoint()) {
                            throw new ConnectException(
                                    "Failed to parse and read a value of type POINT on " + column);
                        }

                        r.deliver(
                                Point.createValue(
                                        fieldDefn.schema(),
                                        mySqlGeometry.getWkb(),
                                        mySqlGeometry.getSrid()));
                    }
                });
    }

    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0,
                (r) -> {
                    Object mutData = data;
                    if (data instanceof java.time.Year) {
                        r.deliver(
                                adjustTemporal(
                                                java.time.Year.of(
                                                        ((java.time.Year) data).getValue()))
                                        .get(ChronoField.YEAR));
                    } else if (data instanceof Date) {
                        r.deliver(((Date) data).getYear() + 1900);
                    } else if (data instanceof String) {
                        mutData = Integer.valueOf((String) data);
                    }

                    if (mutData instanceof Number) {
                        r.deliver(
                                adjustTemporal(java.time.Year.of(((Number) mutData).intValue()))
                                        .get(ChronoField.YEAR));
                    }
                });
    }

    protected Object convertEnumToString(
            List<String> options, Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                "",
                (r) -> {
                    if (data instanceof String) {
                        r.deliver(data);
                    } else if (data instanceof Integer) {
                        if (options != null) {
                            int value = (Integer) data;
                            if (value == 0) {
                                r.deliver("");
                            }

                            int index = value - 1;
                            if (index < options.size() && index >= 0) {
                                r.deliver(options.get(index));
                            }
                        } else {
                            r.deliver((Object) null);
                        }
                    }
                });
    }

    protected Object convertSetToString(
            List<String> options, Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                "",
                (r) -> {
                    if (data instanceof String) {
                        r.deliver(data);
                    } else if (data instanceof Long) {
                        long indexes = (Long) data;
                        r.deliver(this.convertSetValue(column, indexes, options));
                    }
                });
    }

    protected String convertSetValue(Column column, long indexes, List<String> options) {
        StringBuilder sb = new StringBuilder();
        int index = 0;
        boolean first = true;

        for (int optionLen = options.size(); indexes != 0L; indexes >>>= 1) {
            if (indexes % 2L != 0L) {
                if (first) {
                    first = false;
                } else {
                    sb.append(',');
                }

                if (index < optionLen) {
                    sb.append((String) options.get(index));
                } else {
                    this.logger.warn("Found unexpected index '{}' on column {}", index, column);
                }
            }

            ++index;
        }

        return sb.toString();
    }

    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0L,
                (r) -> {
                    if (data instanceof BigDecimal) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedBigint(
                                        (BigDecimal) data));
                    } else if (data instanceof Number) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedBigint(
                                        new BigDecimal(((Number) data).toString())));
                    } else if (data instanceof String) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedBigint(
                                        new BigDecimal((String) data)));
                    } else {
                        r.deliver(this.convertNumeric(column, fieldDefn, data));
                    }
                });
    }

    protected Charset charsetFor(Column column) {
        String mySqlCharsetName = column.charsetName();
        if (mySqlCharsetName == null) {
            logger.warn("Column is missing a character set: {}", column);
            return null;
        }
        String encoding = MySqlConnection.getJavaEncodingForMysqlCharSet(mySqlCharsetName);
        if (encoding == null) {
            logger.debug(
                    "Column uses MySQL character set '{}', which has no mapping to a Java character set, will try it in lowercase",
                    mySqlCharsetName);
            encoding =
                    MySqlConnection.getJavaEncodingForMysqlCharSet(mySqlCharsetName.toLowerCase());
        }
        if (encoding == null) {
            logger.warn(
                    "Column uses MySQL character set '{}', which has no mapping to a Java character set",
                    mySqlCharsetName);
        } else {
            try {
                return Charset.forName(encoding);
            } catch (IllegalCharsetNameException e) {
                logger.error(
                        "Unable to load Java charset '{}' for column with MySQL character set '{}'",
                        encoding,
                        mySqlCharsetName);
            }
        }
        return null;
    }

    protected boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        } else {
            return upperCaseMatch.equals(upperCaseTypeName)
                    || upperCaseTypeName.startsWith(upperCaseMatch + "(");
        }
    }

    protected Object convertDurationToMicroseconds(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0L,
                (r) -> {
                    try {
                        if (data instanceof Duration) {
                            r.deliver(((Duration) data).toNanos() / 1000L);
                        }
                    } catch (IllegalArgumentException var3) {
                    }
                });
    }

    protected Object convertUnsignedInt(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0L,
                (r) -> {
                    if (data instanceof Long) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedInteger((Long) data));
                    } else if (data instanceof Number) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedInteger(
                                        ((Number) data).longValue()));
                    } else {
                        r.deliver(this.convertBigInt(column, fieldDefn, data));
                    }
                });
    }

    protected Object convertUnsignedMediumint(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0,
                (r) -> {
                    if (data instanceof Integer) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedMediumint(
                                        (Integer) data));
                    } else if (data instanceof Number) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedMediumint(
                                        ((Number) data).intValue()));
                    } else {
                        r.deliver(this.convertInteger(column, fieldDefn, data));
                    }
                });
    }

    protected Object convertUnsignedSmallint(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                0,
                (r) -> {
                    if (data instanceof Integer) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedSmallint(
                                        (Integer) data));
                    } else if (data instanceof Number) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedSmallint(
                                        ((Number) data).intValue()));
                    } else {
                        r.deliver(this.convertInteger(column, fieldDefn, data));
                    }
                });
    }

    protected Object convertUnsignedTinyint(Column column, Field fieldDefn, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                Short.valueOf((short) 0),
                (r) -> {
                    if (data instanceof Short) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedTinyint((Short) data));
                    } else if (data instanceof Number) {
                        r.deliver(
                                MySqlUnsignedIntegerConverter.convertUnsignedTinyint(
                                        ((Number) data).shortValue()));
                    } else {
                        r.deliver(this.convertSmallInt(column, fieldDefn, data));
                    }
                });
    }

    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {
        MySqlGeometry empty = MySqlGeometry.createEmpty();
        return this.convertValue(
                column,
                fieldDefn,
                data,
                Geometry.createValue(fieldDefn.schema(), empty.getWkb(), empty.getSrid()),
                (r) -> {
                    if (data instanceof byte[] && data instanceof byte[]) {
                        MySqlGeometry mySqlGeometry = MySqlGeometry.fromBytes((byte[]) data);
                        r.deliver(
                                Geometry.createValue(
                                        fieldDefn.schema(),
                                        mySqlGeometry.getWkb(),
                                        mySqlGeometry.getSrid()));
                    }
                });
    }

    protected boolean isGeometryCollection(String upperCaseTypeName) {
        if (upperCaseTypeName == null) {
            return false;
        } else {
            return upperCaseTypeName.equals("GEOMETRYCOLLECTION")
                    || upperCaseTypeName.equals("GEOMCOLLECTION")
                    || upperCaseTypeName.endsWith(".GEOMCOLLECTION");
        }
    }

    protected String extractEnumAndSetOptionsAsString(Column column) {
        return Strings.join(",", this.extractEnumAndSetOptions(column));
    }

    protected List<String> extractEnumAndSetOptions(Column column) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
    }

    public static void defaultParsingErrorHandler(String message, Exception exception) {
        throw new DebeziumException(message, exception);
    }

    protected Object convertString(
            Column column, Field fieldDefn, Charset columnCharset, Object data) {
        return this.convertValue(
                column,
                fieldDefn,
                data,
                "",
                (r) -> {
                    if (data instanceof byte[]) {
                        r.deliver(new String((byte[]) data, columnCharset));
                    } else if (data instanceof String) {
                        r.deliver(data);
                    }
                });
    }

    protected Object convertTimestampToLocalDateTime(Column column, Field fieldDefn, Object data) {
        if (data == null && !fieldDefn.schema().isOptional()) {
            return null;
        } else {
            return !(data instanceof Timestamp) ? data : ((Timestamp) data).toLocalDateTime();
        }
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Strings.asDuration((String) data);
        }
        return super.convertTime(column, fieldDefn, data);
    }
}
