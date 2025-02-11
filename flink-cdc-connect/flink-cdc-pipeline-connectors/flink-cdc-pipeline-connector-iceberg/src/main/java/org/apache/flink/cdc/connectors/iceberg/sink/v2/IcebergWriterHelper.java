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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.iceberg.sink.utils.IcebergTypeUtils;
import org.apache.flink.table.data.RowData;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/** A Util class for {@link IcebergWriter} to convert {@link Event} into {@link RowData}. */
public class IcebergWriterHelper {

    /** Create a list of {@link RecordData.FieldGetter} for the given {@link Schema}. */
    public static List<RecordData.FieldGetter> createFieldGetters(Schema schema, ZoneId zoneId) {
        List<Column> columns = schema.getColumns();
        List<RecordData.FieldGetter> fieldGetters = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            fieldGetters.add(
                    IcebergTypeUtils.createFieldGetter(columns.get(i).getType(), i, zoneId));
        }
        return fieldGetters;
    }
}
