/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source.deserializer.rowdata;

import org.apache.flink.connector.jdbc.internal.converter.AbstractJdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.oceanbase.source.deserializer.OceanBaseSnapshotEventDeserializerSchema;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Deserialization schema from OceanBase snapshot data to Flink Table/SQL internal data structure
 * {@link RowData}.
 */
public class OceanBaseSnapshotEventDeserializer
        implements OceanBaseSnapshotEventDeserializerSchema<RowData> {

    private static final long serialVersionUID = -2225753733378348510L;

    private final JdbcRowConverter converter;

    public OceanBaseSnapshotEventDeserializer(RowType rowType) {
        this.converter =
                new AbstractJdbcRowConverter(rowType) {

                    private static final long serialVersionUID = -6223914875301753634L;

                    @Override
                    public String converterName() {
                        return "OceanBase";
                    }
                };
    }

    @Override
    public RowData deserialize(ResultSet rs) {
        try {
            return converter.toInternal(rs);
        } catch (SQLException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
