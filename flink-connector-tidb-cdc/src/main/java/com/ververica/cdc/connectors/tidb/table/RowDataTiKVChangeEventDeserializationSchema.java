/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Cdcpb.Event.Row;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Deserialization schema from TiKV Change Event to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class RowDataTiKVChangeEventDeserializationSchema
        extends RowDataTiKVEventDeserializationSchemaBase
        implements TiKVChangeEventDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    public RowDataTiKVChangeEventDeserializationSchema(
            TiConfiguration tiConf,
            String database,
            String tableName,
            TypeInformation<RowData> resultTypeInfo,
            TiKVMetadataConverter[] metadataConverters,
            RowType physicalDataType) {
        super(tiConf, database, tableName, metadataConverters, physicalDataType);
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
    }

    @Override
    public void deserialize(Row row, Collector<RowData> out) throws Exception {
        if (tableInfo == null) {
            tableInfo = fetchTableInfo();
        }
        final RowKey rowKey = RowKey.decode(row.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        Object[] tikvValues;

        switch (row.getOpType()) {
            case DELETE:
                tikvValues = decodeObjects(row.getOldValue().toByteArray(), handle, tableInfo);
                RowData rowDataDelete =
                        (RowData) physicalConverter.convert(tikvValues, tableInfo, null);
                rowDataDelete.setRowKind(RowKind.DELETE);
                emit(new TiKVMetadataConverter.TiKVRowValue(row), rowDataDelete, out);
                break;
            case PUT:
                try {
                    tikvValues =
                            decodeObjects(
                                    row.getValue().toByteArray(),
                                    RowKey.decode(row.getKey().toByteArray()).getHandle(),
                                    tableInfo);
                    if (row.getOldValue() == null || row.getOldValue().isEmpty()) {
                        RowData rowDataUpdateBefore =
                                (RowData) physicalConverter.convert(tikvValues, tableInfo, null);
                        rowDataUpdateBefore.setRowKind(RowKind.INSERT);
                        emit(new TiKVMetadataConverter.TiKVRowValue(row), rowDataUpdateBefore, out);
                    } else {
                        RowData rowDataUpdateAfter =
                                (RowData) physicalConverter.convert(tikvValues, tableInfo, null);
                        rowDataUpdateAfter.setRowKind(RowKind.UPDATE_AFTER);
                        emit(new TiKVMetadataConverter.TiKVRowValue(row), rowDataUpdateAfter, out);
                    }
                    break;
                } catch (final RuntimeException e) {
                    throw new FlinkRuntimeException(
                            String.format(
                                    "Fail to deserialize row: %s, table: %s",
                                    row, tableInfo.getId()),
                            e);
                }
            default:
                throw new IllegalArgumentException("Unknown Row Op Type: " + row.getOpType());
        }
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
