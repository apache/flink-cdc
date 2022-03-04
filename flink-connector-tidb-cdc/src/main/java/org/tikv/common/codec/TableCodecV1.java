/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.codec;

import org.tikv.common.codec.Codec.IntegerCodec;
import org.tikv.common.exception.RowValueHasMoreColumnException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.ObjectRowImpl;
import org.tikv.common.row.Row;
import org.tikv.common.types.DataType.EncodeType;
import org.tikv.common.types.IntegerType;

import java.util.HashMap;
import java.util.List;

/** Copied from client-java. */
public class TableCodecV1 {
    /** Row layout: colID1, value1, colID2, value2, ..... */
    protected static byte[] encodeRow(
            List<TiColumnInfo> columnInfos, Object[] values, boolean isPkHandle) {
        CodecDataOutput cdo = new CodecDataOutput();

        for (int i = 0; i < columnInfos.size(); i++) {
            TiColumnInfo col = columnInfos.get(i);
            // skip pk is handle case
            if (col.isPrimaryKey() && isPkHandle) {
                continue;
            }
            IntegerCodec.writeLongFully(cdo, col.getId(), false);
            col.getType().encode(cdo, EncodeType.VALUE, values[i]);
        }

        // We could not set nil value into kv.
        if (cdo.toBytes().length == 0) {
            return new byte[] {Codec.NULL_FLAG};
        }

        return cdo.toBytes();
    }

    protected static Object[] decodeObjects(byte[] value, Long handle, TiTableInfo tableInfo) {
        return decodeObjects(value, handle, tableInfo, false);
    }

    protected static Object[] decodeObjects(
            byte[] value, Long handle, TiTableInfo tableInfo, boolean enableTableInfoCheck) {
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        int colSize = tableInfo.getColumns().size();
        HashMap<Long, TiColumnInfo> idToColumn = new HashMap<>(colSize);
        for (TiColumnInfo col : tableInfo.getColumns()) {
            idToColumn.put(col.getId(), col);
        }

        // decode bytes to Map<ColumnID, Data>
        HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
        CodecDataInput cdi = new CodecDataInput(value);
        Object[] res = new Object[colSize];
        while (!cdi.eof()) {
            long colID = (long) IntegerType.BIGINT.decode(cdi);
            TiColumnInfo colInfo = idToColumn.get(colID);
            if (null != colInfo) {
                Object colValue = colInfo.getType().decodeForBatchWrite(cdi);
                decodedDataMap.put(colID, colValue);
            } else {
                if (enableTableInfoCheck) {
                    throw new RowValueHasMoreColumnException();
                }
            }
        }

        // construct Row with Map<ColumnID, Data> & handle
        for (int i = 0; i < colSize; i++) {
            // skip pk is handle case
            TiColumnInfo col = tableInfo.getColumn(i);
            if (col.isPrimaryKey() && tableInfo.isPkHandle()) {
                res[i] = handle;
            } else {
                res[i] = decodedDataMap.get(col.getId());
            }
        }
        return res;
    }

    protected static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
        return ObjectRowImpl.create(decodeObjects(value, handle, tableInfo));
    }
}
