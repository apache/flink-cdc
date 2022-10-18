/*
 * Licensed to Benjamin(li@sellgirl.com)
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * .
 */

package com.ververica.cdc.connectors.mysql.debezium;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.apache.kafka.connect.data.Field;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.temporal.TemporalAdjuster;
import java.util.List;

/** fix bug mysql Datetime timeZone bug on MySqlValueConverters. */
public class PFMySqlValueConverters extends MySqlValueConverters {
    public PFMySqlValueConverters(
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            BigIntUnsignedMode bigIntUnsignedMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode) {
        super(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryMode,
                x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }

    /**
     * Create a new instance that always uses UTC for the default time zone when converting values
     * without timezone information to values that require timezones.
     */
    public PFMySqlValueConverters(
            DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            BigIntUnsignedMode bigIntUnsignedMode,
            CommonConnectorConfig.BinaryHandlingMode binaryMode,
            TemporalAdjuster adjuster,
            ParsingErrorHandler parsingErrorHandler) {
        super(
                decimalMode,
                temporalPrecisionMode,
                bigIntUnsignedMode,
                binaryMode,
                adjuster,
                parsingErrorHandler);
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
                return super.converter(column, fieldDefn);
            case Types.TIME:
                return super.converter(column, fieldDefn);
            case Types.TIMESTAMP:
                return ((ValueConverter)
                                (data -> {
                                    // 为了复盖MySqlValueConverters对Timestamp转为LocalDateTime可能产生的时间误差 --
                                    // benjamin20221114
                                    if (null != data && data instanceof Timestamp) {

                                        return ((Timestamp) data).getTime();
                                    }
                                    return data;
                                }))
                        .and(super.converter(column, fieldDefn));
            default:
                break;
        }

        // Otherwise, let the base class handle it ...
        return super.converter(column, fieldDefn);
    }
}
