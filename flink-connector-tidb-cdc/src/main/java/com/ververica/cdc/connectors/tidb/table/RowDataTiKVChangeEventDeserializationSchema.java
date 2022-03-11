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

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.DifferentColumnCountException;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb.Event.Row;

import static com.ververica.cdc.connectors.tidb.table.utils.TiKVTypeUtils.getObjectsWithDataTypes;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Deserialization schema from TiKV Change Event to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class RowDataTiKVChangeEventDeserializationSchema
        implements TiKVChangeEventDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(RowDataTiKVChangeEventDeserializationSchema.class);

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Configuration of TiKV. * */
    private final TiConfiguration tiConf;

    private final String database;
    private final String tableName;

    /** Information of the TiKV table. * */
    private transient TiTableInfo gTableInfo = null;

    public RowDataTiKVChangeEventDeserializationSchema(
            TypeInformation<RowData> resultTypeInfo,
            TiConfiguration tiConf,
            String database,
            String tableName) {

        this.resultTypeInfo = resultTypeInfo;
        this.tiConf = tiConf;
        this.database = database;
        this.tableName = tableName;
    }

    @Override
    public void deserialize(Row row, Collector<RowData> out) throws Exception {
        int i = 0;
        for (; i < 2; i++) {
            TiTableInfo tableInfo = getTableInfo();
            try {
                final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
                final long handle = rowKey.getHandle();
                switch (row.getOpType()) {
                    case DELETE:
                        out.collect(
                                GenericRowData.ofKind(
                                        RowKind.DELETE,
                                        getObjectsWithDataTypes(
                                                decodeObjects(
                                                        row.getOldValue().toByteArray(),
                                                        handle,
                                                        tableInfo,
                                                        true),
                                                tableInfo)));
                        break;
                    case PUT:
                        try {
                            if (row.getOldValue() == null) {
                                out.collect(
                                        GenericRowData.ofKind(
                                                RowKind.INSERT,
                                                getRowDataFields(
                                                        row.getValue().toByteArray(),
                                                        handle,
                                                        tableInfo)));

                            } else {
                                // TODO TiKV cdc client doesn't return old value in PUT event
                                //                        if (!row.getOldValue().isEmpty()) {
                                //                            out.collect(
                                //                                    GenericRowData.ofKind(
                                //                                            RowKind.UPDATE_BEFORE,
                                //                                            getRowDataFields(
                                //
                                // row.getOldValue().toByteArray(),
                                //                                                    handle,
                                //                                                    tableInfo)));
                                //                        }
                                out.collect(
                                        GenericRowData.ofKind(
                                                RowKind.UPDATE_AFTER,
                                                getRowDataFields(
                                                        row.getValue().toByteArray(),
                                                        handle,
                                                        tableInfo)));
                            }
                            break;
                        } catch (DifferentColumnCountException e) {
                            throw e;
                        } catch (final RuntimeException e) {
                            throw new FlinkRuntimeException(
                                    String.format(
                                            "Fail to deserialize row: %s, table: %s",
                                            row, tableInfo.getId()),
                                    e);
                        }
                    default:
                        throw new IllegalArgumentException(
                                "Unknown Row Op Type: " + row.getOpType().toString());
                }
                break;
            } catch (DifferentColumnCountException e) {
                LOG.warn(
                        "Row value and TableInfo has different column count, reload TableInfo and try again");
                initTableInfo();
            }
        }

        if (2 == i) {
            LOG.warn("Row value is lost, value: {}", row);
            throw new FlinkRuntimeException(String.format("row value is lost, %s", row));
        }
    }

    private TiTableInfo getTableInfo() {
        if (null == gTableInfo) {
            initTableInfo();
        }
        return gTableInfo;
    }

    private synchronized void initTableInfo() {
        try (TiSession session = TiSession.create(tiConf)) {
            gTableInfo = session.getCatalog().getTable(database, tableName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Object[] getRowDataFields(byte[] value, Long handle, TiTableInfo tableInfo) {
        return getObjectsWithDataTypes(decodeObjects(value, handle, tableInfo), tableInfo);
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
