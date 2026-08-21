/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Types;
import java.time.ZoneOffset;
import java.util.Locale;

/**
 * Conversion of DB2 specific datatypes.
 *
 * <p>This class intentionally shadows Debezium 1.9.8's Db2ValueConverters because that version does
 * not register a converter for DB2 DECFLOAT, which is reported by the IBM JDBC driver as {@link
 * Types#OTHER}. Without this override Debezium drops DECFLOAT columns from data events while Flink
 * CDC still exposes them in pipeline schema discovery.
 */
public class Db2ValueConverters extends JdbcValueConverters {

    private static final String DECFLOAT = "DECFLOAT";

    public Db2ValueConverters() {}

    public Db2ValueConverters(
            DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, so store as int16
                return SchemaBuilder.int16();
            default:
                if (isDecfloat(column)) {
                    return SchemaBuilder.float64();
                }
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.TINYINT:
                // values are an 8-bit unsigned integer value between 0 and 255, so store as int16
                return (data) -> convertSmallInt(column, fieldDefn, data);
            default:
                if (isDecfloat(column)) {
                    return (data) -> convertDouble(column, fieldDefn, data);
                }
                return super.converter(column, fieldDefn);
        }
    }

    /** Time precision in DB2 is defined in scale, the default one is 7. */
    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().get();
    }

    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampWithZone(column, fieldDefn, data);
    }

    private static boolean isDecfloat(Column column) {
        return column.jdbcType() == Types.OTHER
                && column.typeName() != null
                && column.typeName().toUpperCase(Locale.ROOT).startsWith(DECFLOAT);
    }
}
