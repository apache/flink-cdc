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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;

import java.time.ZoneId;
import java.util.List;

/** Keep a list of {@link RecordData.FieldGetter} for a specific {@link Schema}. */
public class TableSchemaInfo {

    private final Schema schema;

    private final List<RecordData.FieldGetter> fieldGetters;

    private final boolean hasPrimaryKey;

    public TableSchemaInfo(Schema schema, ZoneId zoneId) {
        this.schema = schema;
        this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId);
        this.hasPrimaryKey = !schema.primaryKeys().isEmpty();
    }

    public Schema getSchema() {
        return schema;
    }

    public List<RecordData.FieldGetter> getFieldGetters() {
        return fieldGetters;
    }

    public boolean hasPrimaryKey() {
        return hasPrimaryKey;
    }
}
