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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.fluss.utils.FlussUtils;

import java.time.ZoneId;

public class TableSchemaInfo {
    public final Schema schema;

    public final RecordData.FieldGetter[] fieldGetters;

    public TableSchemaInfo(Schema schema, ZoneId zoneId) {
        this.schema = schema;
        fieldGetters = new RecordData.FieldGetter[schema.getColumnCount()];
        for (int i = 0; i < schema.getColumnCount(); i++) {
            fieldGetters[i] =
                    FlussUtils.createFieldGetter(schema.getColumns().get(i).getType(), i, zoneId);
        }
    }
}
