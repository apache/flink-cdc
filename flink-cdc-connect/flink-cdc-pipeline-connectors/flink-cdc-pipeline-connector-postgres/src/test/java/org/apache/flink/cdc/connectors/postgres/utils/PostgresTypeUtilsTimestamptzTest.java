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

package org.apache.flink.cdc.connectors.postgres.utils;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;

import io.debezium.connector.postgresql.PgOid;
import io.debezium.relational.Column;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that TIMESTAMPTZ maps to TIMESTAMP_WITH_LOCAL_TIME_ZONE (not TIMESTAMP_WITH_TIME_ZONE),
 * aligning with the Debezium deserializer's converter which produces LocalZonedTimestampData for
 * TIMESTAMPTZ columns.
 *
 * <p>Using TIMESTAMP_WITH_TIME_ZONE (ZonedTimestampType) causes a type mismatch: the BinaryWriter
 * expects ZonedTimestampData but the deserializer produces LocalZonedTimestampData, resulting in
 * binary data corruption and NumberFormatException in the Iceberg sink's
 * IcebergTypeUtils.createFieldGetter.
 */
public class PostgresTypeUtilsTimestamptzTest {

    @Test
    public void testTimestamptzMapsToLocalZonedTimestamp() {
        Column column =
                Column.editor()
                        .name("created_at")
                        .jdbcType(java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
                        .nativeType(PgOid.TIMESTAMPTZ)
                        .type("timestamptz")
                        .length(Column.UNSET_INT_VALUE)
                        .scale(6)
                        .optional(true)
                        .create();

        // Pass null for dbzConfig and typeRegistry since the TIMESTAMPTZ code path
        // only uses the column's nativeType and scale.
        DataType result = PostgresTypeUtils.fromDbzColumn(column, null, null);

        assertThat(result.getTypeRoot())
                .as(
                        "TIMESTAMPTZ should map to TIMESTAMP_WITH_LOCAL_TIME_ZONE, "
                                + "not TIMESTAMP_WITH_TIME_ZONE, to match the Debezium deserializer")
                .isEqualTo(DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        assertThat(result).isInstanceOf(LocalZonedTimestampType.class);
    }
}
