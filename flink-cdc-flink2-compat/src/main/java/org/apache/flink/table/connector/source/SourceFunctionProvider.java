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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Deprecated
@Internal
public interface SourceFunctionProvider
        extends org.apache.flink.legacy.table.connector.source.SourceFunctionProvider {

    static org.apache.flink.legacy.table.connector.source.SourceFunctionProvider of(
            SourceFunction<RowData> sourceFunction, boolean isBounded) {
        return of(sourceFunction, isBounded, (Integer) null);
    }

    static org.apache.flink.legacy.table.connector.source.SourceFunctionProvider of(
            final SourceFunction<RowData> sourceFunction,
            final boolean isBounded,
            @Nullable final Integer sourceParallelism) {
        return new SourceFunctionProvider() {
            public org.apache.flink.streaming.api.functions.source.legacy.SourceFunction<RowData>
                    createSourceFunction() {
                return sourceFunction;
            }

            public boolean isBounded() {
                return isBounded;
            }

            public Optional<Integer> getParallelism() {
                return Optional.ofNullable(sourceParallelism);
            }
        };
    }

    org.apache.flink.streaming.api.functions.source.legacy.SourceFunction<RowData>
            createSourceFunction();
}
