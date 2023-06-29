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

package com.ververica.cdc.connectors.postgres.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.connector.postgresql.PostgresOffsetContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Utils for handling {@link PostgresOffset}. */
public class PostgresOffsetUtils {
    public static PostgresOffsetContext getPostgresOffsetContext(
            PostgresOffsetContext.Loader loader, Offset offset) {

        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // all the keys happen to be long type for PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap = new HashMap<>();
        for (String key : offsetStrMap.keySet()) {
            String value = offsetStrMap.get(key);
            if (value != null) {
                offsetMap.put(key, Long.parseLong(value));
            }
        }
        return loader.load(offsetMap);
    }
}
