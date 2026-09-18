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

package io.debezium.connector.db2;

import io.debezium.relational.Column;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class Db2ValueConvertersTest {

    private final Db2ValueConverters converters = new Db2ValueConverters();

    @Test
    void shouldExposeDecfloatAsDoubleSchema() {
        Schema schema = converters.schemaBuilder(decfloatColumn("C_DECFLOAT16")).build();

        assertThat(schema.type()).isEqualTo(Schema.Type.FLOAT64);
    }

    @Test
    void shouldConvertDecfloatValueToDouble() {
        Object converted =
                converters
                        .converter(decfloatColumn("C_DECFLOAT34"), null)
                        .convert(new BigDecimal("12345.6789"));

        assertThat(converted).isEqualTo(12345.6789d);
    }

    private static Column decfloatColumn(String name) {
        return Column.editor()
                .name(name)
                .position(1)
                .jdbcType(Types.OTHER)
                .type("DECFLOAT")
                .optional(true)
                .create();
    }
}
