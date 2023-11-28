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

package com.ververica.cdc.common.utils;

import org.apache.flink.table.data.GenericRowData;

import com.ververica.cdc.common.data.RecordData;

import java.util.List;

/** Utilities for handling {@link RecordData}s. */
public class RecordDataUtils {
    /** convert recordData to Flink GenericRowData */
    public static GenericRowData toFlinkRowData(
            RecordData recordData, List<RecordData.FieldGetter> fieldGetters) {
        Preconditions.checkState(fieldGetters.size() == recordData.getArity());
        GenericRowData rowData = new GenericRowData(recordData.getArity());
        for (int i = 0; i < recordData.getArity(); i++) {
            Object field = fieldGetters.get(i).getFieldOrNull(recordData);
            rowData.setField(i, field);
        }
        return rowData;
    }
}
