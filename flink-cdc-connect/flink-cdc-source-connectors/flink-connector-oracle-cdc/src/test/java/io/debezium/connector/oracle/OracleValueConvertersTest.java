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

package io.debezium.connector.oracle;

import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for Oracle connector-visible value conversion. */
class OracleValueConvertersTest {

    @Test
    void shouldConvertLogMinerUnistrValueWithEmbeddedConcatSequence() {
        ValueConverter converter = converters().converter(nvarcharColumn(), null);

        Object converted =
                converter.convert(
                        "UNISTR('\\4E2D\\56FD||\\6B66\\6C49')");

        assertThat(converted).isEqualTo("\u4E2D\u56FD||\u6B66\u6C49");
    }

    @Test
    void shouldConvertConcatenatedLogMinerUnistrValues() {
        ValueConverter converter = converters().converter(nvarcharColumn(), null);

        Object converted =
                converter.convert(
                        "UNISTR('\\4E2D\\56FD')||UNISTR('\\6B66\\6C49')");

        assertThat(converted).isEqualTo("\u4E2D\u56FD\u6B66\u6C49");
    }

    private static OracleValueConverters converters() {
        Configuration config =
                Configuration.create()
                        .with("database.server.name", "test")
                        .with("database.dbname", "ORCLCDB")
                        .with("database.hostname", "127.0.0.1")
                        .with("database.port", 1521)
                        .with("database.user", "flinkuser")
                        .with("database.password", "flinkpw")
                        .build();
        return new OracleValueConverters(new OracleConnectorConfig(config), null);
    }

    private static Column nvarcharColumn() {
        return Column.editor()
                .name("VAL_NVARCHAR2")
                .jdbcType(Types.NVARCHAR)
                .type("NVARCHAR2")
                .length(100)
                .optional(false)
                .create();
    }
}
