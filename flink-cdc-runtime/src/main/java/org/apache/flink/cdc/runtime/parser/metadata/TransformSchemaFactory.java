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

package org.apache.flink.cdc.runtime.parser.metadata;

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.utils.StringUtils;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** TransformSchemaFactory to generate the metadata of calcite. */
public class TransformSchemaFactory implements SchemaFactory {

    public static final TransformSchemaFactory INSTANCE = new TransformSchemaFactory();

    private TransformSchemaFactory() {}

    @Override
    public Schema create(SchemaPlus schemaPlus, String schemaName, Map<String, Object> operand) {
        if (StringUtils.isNullOrWhitespaceOnly(schemaName)) {
            schemaName = "default_schema";
        }
        String tableName = String.valueOf(operand.get("tableName"));
        List<Column> columns = (List<Column>) operand.get("columns");
        return new TransformSchema(
                schemaName, Arrays.asList(new TransformTable(tableName, columns)));
    }
}
