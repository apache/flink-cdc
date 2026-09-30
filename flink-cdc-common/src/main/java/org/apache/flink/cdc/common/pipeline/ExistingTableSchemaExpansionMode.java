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

package org.apache.flink.cdc.common.pipeline;

import org.apache.flink.cdc.common.annotation.Experimental;

/**
 * Strategy for handling an existing target table when the initial {@code CreateTableEvent} arrives.
 *
 * <p>{@code CHECK} guards the initial table state and is independent of {@link
 * SchemaChangeBehavior}. {@code TRY_EXPAND} and {@code EXPAND} skip the framework-side initial
 * handling when {@link SchemaChangeBehavior} is {@code IGNORE} or {@code EXCEPTION}; subsequent
 * source schema changes are always controlled by {@link SchemaChangeBehavior}.
 */
@Experimental
public enum ExistingTableSchemaExpansionMode {
    /** Do not check or expand the existing target table; keep the sink's original behavior. */
    DISABLED,

    /**
     * Validate that the existing target table can contain every upstream column without executing
     * any DDL. Fails the job with an aggregated error (including suggested repair statements) if
     * the target table is missing or incompatible. This mode guards the initial table state and
     * runs regardless of {@link SchemaChangeBehavior}.
     */
    CHECK,

    /**
     * Check and best-effort expand the existing target table with safe DDL. Failures of this
     * mechanism are logged and delegated to the sink's original behavior.
     *
     * <p>A connector that does not implement {@link
     * org.apache.flink.cdc.common.sink.ExistingTableSchemaExpansionSupport} is a configuration
     * error and fails the job rather than silently degrading.
     */
    TRY_EXPAND,

    /**
     * Check and expand the existing target table with safe DDL. Any incompatibility or failure of
     * this mechanism fails the job.
     */
    EXPAND
}
