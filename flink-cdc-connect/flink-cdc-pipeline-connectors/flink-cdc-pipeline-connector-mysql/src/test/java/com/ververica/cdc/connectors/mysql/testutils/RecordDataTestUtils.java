/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.testutils;

import com.ververica.cdc.common.data.RecordData;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.RowType;

import java.util.List;

/** Utility for {@link RecordData}. */
public class RecordDataTestUtils {

    public static Object[] recordFields(RecordData record, RowType rowType) {
        int fieldNum = record.getArity();
        List<DataType> fieldTypes = rowType.getChildren();
        Object[] fields = new Object[fieldNum];
        for (int i = 0; i < fieldNum; i++) {
            if (record.isNullAt(i)) {
                fields[i] = null;
            } else {
                DataType type = fieldTypes.get(i);
                RecordData.FieldGetter fieldGetter = RecordData.createFieldGetter(type, i);
                Object o = fieldGetter.getFieldOrNull(record);
                fields[i] = o;
            }
        }
        return fields;
    }

    private RecordDataTestUtils() {}
}
