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

package org.apache.flink.cdc.connectors.iceberg.sink.utils;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.util.List;

/** Utils for convertion of {@link RowData} and {@link DataChangeEvent}. */
public class RowDataUtils {

    /** Convert {@link DataChangeEvent} to {@link RowData}. */
    public static RowData convertDataChangeEventToRowData(
            DataChangeEvent dataChangeEvent, List<RecordData.FieldGetter> fieldGetters) {
        RecordData recordData;
        RowKind kind;
        switch (dataChangeEvent.op()) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                {
                    recordData = dataChangeEvent.after();
                    kind = RowKind.INSERT;
                    break;
                }
            case DELETE:
                {
                    recordData = dataChangeEvent.before();
                    kind = RowKind.DELETE;
                    break;
                }
            default:
                throw new IllegalArgumentException("don't support type of " + dataChangeEvent.op());
        }
        GenericRowData genericRowData = new GenericRowData(recordData.getArity());
        genericRowData.setRowKind(kind);
        for (int i = 0; i < recordData.getArity(); i++) {
            genericRowData.setField(i, fieldGetters.get(i).getFieldOrNull(recordData));
        }
        return genericRowData;
    }
}
