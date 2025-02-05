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

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** DefaultValueConverter for OceanBase. */
public class OceanBaseDefaultValueConverter implements DefaultValueConverter {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseDefaultValueConverter.class);

    private final OceanBaseValueConverters valueConverters;

    public OceanBaseDefaultValueConverter(OceanBaseValueConverters valueConverters) {
        this.valueConverters = valueConverters;
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        if (defaultValueExpression == null) {
            return Optional.empty();
        }
        String defaultValue = defaultValueExpression.trim();

        final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
        if (schemaBuilder == null) {
            return Optional.of(defaultValue);
        }

        final Schema schema = schemaBuilder.build();

        // In order to get the valueConverter for this column, we have to create a field;
        // The index value -1 in the field will never be used when converting default value;
        // So we can set any number here;
        final Field field = new Field(column.name(), -1, schema);
        final ValueConverter valueConverter = valueConverters.converter(column, field);

        return Optional.ofNullable(valueConverter.convert(defaultValue));
    }
}
