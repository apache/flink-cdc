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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.parser.metadata.MetadataColumns;

import javax.annotation.Nullable;

import java.util.Map;

/** Contextual information during Post-transform phase. */
public class TransformContext {

    public long epochTime;

    public String opType;

    public Map<String, String> meta;

    /**
     * Retrieve a corresponding object based on identifier name. The lookup order would be: <br>
     * 1. Built-in metadata column names; <br>
     * 2. Source-provided metadata column names; <br>
     * 3. Calculated column names (which may shade original columns); <br>
     * 4. Existing upstream column names. <br>
     * If given name is nowhere to be found, an exception will be thrown.
     */
    public static Object lookupObjectByName(
            String name,
            PostTransformChangeInfo tableInfo,
            Map<String, SupportedMetadataColumn> supportedMetadataColumns,
            Object[] preRow,
            @Nullable Object[] postRow,
            TransformContext context) {
        switch (name) {
            case MetadataColumns.DEFAULT_NAMESPACE_NAME:
                return tableInfo.getNamespace();
            case MetadataColumns.DEFAULT_SCHEMA_NAME:
                return tableInfo.getSchemaName();
            case MetadataColumns.DEFAULT_TABLE_NAME:
                return tableInfo.getTableName();
            case MetadataColumns.DEFAULT_DATA_EVENT_TYPE:
                return context.opType;
        }

        // or source-provided metadata column
        if (supportedMetadataColumns.containsKey(name)) {
            return supportedMetadataColumns.get(name).read(context.meta);
        }

        if (postRow != null) {
            // or a column that is presented in post-transform schema
            @Nullable
            Integer indexInPostTransformedSchema =
                    tableInfo.getPostTransformedSchemaFieldIndex(name);
            if (indexInPostTransformedSchema != null) {
                return postRow[indexInPostTransformedSchema];
            }
        }

        // or a pre-transformed column that has been projected out
        @Nullable
        Integer indexInPreTransformedSchema = tableInfo.getPreTransformedSchemaFieldIndex(name);
        if (indexInPreTransformedSchema != null) {
            return preRow[indexInPreTransformedSchema];
        }

        throw new RuntimeException("Failed to lookup column name: " + name);
    }
}
