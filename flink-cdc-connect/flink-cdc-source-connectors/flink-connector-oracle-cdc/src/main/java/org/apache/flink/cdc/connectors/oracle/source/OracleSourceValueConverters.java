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

package org.apache.flink.cdc.connectors.oracle.source;

import org.apache.flink.cdc.connectors.oracle.source.converters.GeometryConverter;
import org.apache.flink.cdc.connectors.oracle.source.converters.OracleGeometry;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import oracle.jdbc.OracleTypes;
import oracle.sql.STRUCT;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Types;

/** Oracle Value Converters for cdc. */
public class OracleSourceValueConverters extends OracleValueConverters {

    public OracleSourceValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config, connection);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column.typeName() != null && column.typeName().equals("SDO_GEOMETRY")) {
            return io.debezium.data.geometry.Geometry.builder();
        }

        switch (column.jdbcType()) {
            case Types.FLOAT:
            case OracleTypes.BINARY_FLOAT:
                logger.debug(
                        "Building schema for column {} of type {} named {} with constraints ({},{})",
                        column.name(),
                        column.jdbcType(),
                        column.typeName(),
                        column.length(),
                        column.scale());
                return SchemaBuilder.float32();
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        if (column.typeName() != null && column.typeName().equals("SDO_GEOMETRY")) {
            return (data -> convertGeometry(column, fieldDefn, data));
        }
        switch (column.jdbcType()) {
            case OracleTypes.BINARY_FLOAT:
            case Types.FLOAT:
                // When the value instance of float,convert it to float type.
                return data -> convertFloat(column, fieldDefn, data);
        }

        return super.converter(column, fieldDefn);
    }

    /**
     * Convert the a value representing a GEOMETRY {@code STRUCT} value to a Geometry value used in
     * a {@link SourceRecord}.
     *
     * @param column the column in which the value appears
     * @param fieldDefn the field definition for the {@link SourceRecord}'s {@link Schema}; never
     *     null
     * @param data the data; may be null
     * @return the converted value, or null if the conversion could not be made and the column
     *     allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not
     *     allow nulls
     */
    protected Object convertGeometry(Column column, Field fieldDefn, Object data) {

        final OracleGeometry empty = OracleGeometry.createEmpty();
        return convertValue(
                column,
                fieldDefn,
                data,
                io.debezium.data.geometry.Geometry.createValue(
                        fieldDefn.schema(), empty.getWkb(), empty.getSrid()),
                (r) -> {
                    if (data instanceof STRUCT) {
                        GeometryConverter geometryConverter = new GeometryConverter();
                        OracleGeometry mySqlGeometry =
                                geometryConverter.convertSTRUCTToWKT((STRUCT) data);
                        r.deliver(
                                io.debezium.data.geometry.Geometry.createValue(
                                        fieldDefn.schema(),
                                        mySqlGeometry.getWkb(),
                                        mySqlGeometry.getSrid()));
                    }
                });
    }
}
