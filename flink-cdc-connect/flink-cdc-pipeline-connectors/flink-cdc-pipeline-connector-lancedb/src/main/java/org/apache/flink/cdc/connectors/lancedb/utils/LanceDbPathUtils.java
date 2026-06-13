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

package org.apache.flink.cdc.connectors.lancedb.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkConfig;

/** Resolves source table IDs to target Lance dataset paths. */
public class LanceDbPathUtils {

    private LanceDbPathUtils() {}

    public static String resolveDatasetPath(TableId tableId, LanceDbDataSinkConfig config) {
        String mapped = config.getTablePathMapping().get(tableId);
        if (mapped != null && !mapped.trim().isEmpty()) {
            return trimTrailingSlash(mapped.trim());
        }
        String tableName = tableId.toString();
        String datasetName =
                config.isTableNameNormalizeEnabled()
                        ? LanceDbNameUtils.normalizeSimpleName(tableName, "dataset name")
                        : LanceDbNameUtils.validateSimpleName(tableName, "dataset name");
        if (!datasetName.endsWith(".lance")) {
            datasetName = datasetName + ".lance";
        }
        return trimTrailingSlash(config.getRootPath()) + "/" + datasetName;
    }

    public static String trimTrailingSlash(String value) {
        String result = value;
        while (result.length() > 1 && result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }
}
