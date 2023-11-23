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

import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.RecordData;
import org.apache.flink.table.data.GenericRowData;

/** Utilities for handling {@link RecordData}s. */
public class RecordDataUtils {
    public static GenericRowData toFlinkRowData(GenericRecordData recordData){
        GenericRowData rowData = new GenericRowData(recordData.getArity());
        for(int i=0; i< recordData.getArity(); i++){
            rowData.setField(i, recordData.getField(i));
        }
        return rowData;
    }
}
