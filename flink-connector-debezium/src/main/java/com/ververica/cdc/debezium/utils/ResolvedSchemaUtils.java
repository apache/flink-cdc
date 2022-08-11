/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.debezium.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.stream.Collectors;

/** Utilities to {@link ResolvedSchema}. */
@Internal
public class ResolvedSchemaUtils {
    private ResolvedSchemaUtils() {}

    /** Return {@link ResolvedSchema} which consists of all physical columns. */
    public static ResolvedSchema getPhysicalSchema(ResolvedSchema resolvedSchema) {
        return new ResolvedSchema(
                resolvedSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList()),
                resolvedSchema.getWatermarkSpecs(),
                resolvedSchema.getPrimaryKey().orElse(null));
    }
}
