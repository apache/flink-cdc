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

import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Kvrpcpb.KvPair;

import static com.ververica.cdc.connectors.tidb.table.utils.TiKVTypeUtils.getObjectsWithDataTypes;
import static org.tikv.common.codec.TableCodec.decodeObjects;

/**
 * Deserialization schema from TiKV Snapshot Event to Flink Table/SQL internal data structure {@link
 * RowData}.
 */
public class RowDataTiKVSnapshotEventDeserializationSchema
        implements TiKVSnapshotEventDeserializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    /** TypeInformation of the produced {@link RowData}. * */
    private final TypeInformation<RowData> resultTypeInfo;

    /** Information of the TiKV table. * */
    private final TiTableInfo tableInfo;

    public RowDataTiKVSnapshotEventDeserializationSchema(
            TypeInformation<RowData> resultTypeInfo, TiTableInfo tableInfo) {
        this.resultTypeInfo = resultTypeInfo;
        this.tableInfo = tableInfo;
    }

    @Override
    public void deserialize(KvPair record, Collector<RowData> out) throws Exception {
        final RowKey rowKey = RowKey.decode(record.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        out.collect(
                GenericRowData.ofKind(
                        RowKind.INSERT,
                        getRowDataFields(record.getValue().toByteArray(), handle, tableInfo)));
    }

    private static Object[] getRowDataFields(byte[] value, Long handle, TiTableInfo tableInfo) {
        return getObjectsWithDataTypes(decodeObjects(value, handle, tableInfo), tableInfo);
    }

    @Override
    public TypeInformation getProducedType() {
        return resultTypeInfo;
    }
}
