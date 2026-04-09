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

package org.apache.flink.cdc.connectors.oracle.source.utils;

import org.apache.flink.table.api.DataTypes;

import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OracleTypeUtils}. */
class OracleTypeUtilsTest {

    @Test
    void shouldMapOracleUserDefinedTypeToString() {
        Column column =
                Column.editor()
                        .name("ADDRESS")
                        .jdbcType(Types.OTHER)
                        .type("CUST_ADDRESS_TYP", "CUST_ADDRESS_TYP")
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column)).isEqualTo(DataTypes.STRING().notNull());
    }

    @Test
    void shouldMapDateToTimestamp() {
        Column column =
                Column.editor()
                        .name("CREATE_TIME")
                        .jdbcType(Types.DATE)
                        .type("DATE")
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column))
                .isEqualTo(DataTypes.TIMESTAMP().notNull());
    }

    @Test
    void shouldMapNumberOneToBoolean() {
        Column column =
                Column.editor()
                        .name("IS_ACTIVE")
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER", "NUMBER")
                        .length(1)
                        .scale(0)
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column)).isEqualTo(DataTypes.BOOLEAN().notNull());
    }

    @Test
    void shouldMapIntegerNumberToBigintByDigits() {
        Column column =
                Column.editor()
                        .name("ORDER_ID")
                        .jdbcType(Types.NUMERIC)
                        .type("NUMBER", "NUMBER")
                        .length(10)
                        .scale(0)
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column)).isEqualTo(DataTypes.BIGINT().notNull());
    }

    @Test
    void shouldMapTimestampWithTimezonePrecision() {
        Column column =
                Column.editor()
                        .name("EVENT_TIME")
                        .jdbcType(Types.TIMESTAMP_WITH_TIMEZONE)
                        .type("TIMESTAMP(3) WITH TIME ZONE", "TIMESTAMP(3) WITH TIME ZONE")
                        .length(3)
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column))
                .isEqualTo(DataTypes.TIMESTAMP_WITH_TIME_ZONE(3).notNull());
    }

    @Test
    void shouldMapTimestampLocalTimezonePrecision() {
        Column column =
                Column.editor()
                        .name("UPDATE_TIME")
                        .jdbcType(oracle.jdbc.OracleTypes.TIMESTAMPLTZ)
                        .type(
                                "TIMESTAMP(6) WITH LOCAL TIME ZONE",
                                "TIMESTAMP(6) WITH LOCAL TIME ZONE")
                        .length(6)
                        .optional(false)
                        .create();

        assertThat(OracleTypeUtils.fromDbzColumn(column))
                .isEqualTo(DataTypes.TIMESTAMP_LTZ(6).notNull());
    }
}
