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

package org.tikv.common.codec;

import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.types.DataType;
import org.tikv.common.types.MySQLType;

/** Decodes TiDB RowV2 values without converting binary strings to UTF-8 text. */
public final class TiDBRowV2Decoder {

    private TiDBRowV2Decoder() {}

    public static Object[] decodeObjectsPreservingBinary(
            byte[] value, Long handle, TiTableInfo tableInfo) {
        if (handle == null && tableInfo.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        RowV2 row = RowV2.createNew(value);
        Object[] result = new Object[tableInfo.getColumns().size()];
        for (TiColumnInfo column : tableInfo.getColumns()) {
            int offset = column.getOffset();
            if (column.isPrimaryKey() && tableInfo.isPkHandle()) {
                result[offset] = handle;
                continue;
            }

            RowV2.ColIDSearchResult searchResult = row.findColID(column.getId());
            if (searchResult.isNull || searchResult.notFound) {
                continue;
            }

            byte[] columnData = row.getData(searchResult.idx);
            result[offset] =
                    isBinaryString(column.getType())
                            ? columnData
                            : RowDecoderV2.decodeCol(columnData, column.getType());
        }
        return result;
    }

    private static boolean isBinaryString(DataType type) {
        MySQLType mysqlType = type.getType();
        return (type.isBinary() || "binary".equalsIgnoreCase(type.getCharset()))
                && (mysqlType == MySQLType.TypeString
                        || mysqlType == MySQLType.TypeVarchar
                        || mysqlType == MySQLType.TypeVarString);
    }
}
