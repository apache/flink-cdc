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

package org.apache.hudi.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** Rewrite to avoid using deprecated API. */
public class CatalogUtils {
    public static CatalogTable createCatalogTable(
            Schema schema,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable String comment) {
        return CatalogTable.newBuilder()
                .schema(schema)
                .comment(comment)
                .partitionKeys(partitionKeys)
                .options(options)
                .build();
    }
}
