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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;

import java.util.List;
import java.util.Map;

/**
 * Compatibility adapter for {@link CatalogTable} in Flink 1.20.
 *
 * <p>This adapter provides a factory method to create CatalogTable instances compatible with Flink
 * 1.x.
 */
@Internal
public class CatalogTableAdapter {

    /**
     * Creates a CatalogTable using the Flink 1.x API.
     *
     * @param schema the table schema
     * @param comment the table comment
     * @param partitionKeys the partition keys
     * @param options the table options
     * @return a new CatalogTable instance
     */
    public static CatalogTable of(
            Schema schema,
            String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        return CatalogTable.of(schema, comment, partitionKeys, options);
    }
}
