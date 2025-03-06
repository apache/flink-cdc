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

package org.apache.flink.cdc.connectors.base.config;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;

import java.io.Serializable;

/** The source configuration which offers basic source configuration. */
@Experimental
public interface SourceConfig extends Serializable {

    StartupOptions getStartupOptions();

    int getSplitSize();

    int getSplitMetaGroupSize();

    boolean isIncludeSchemaChanges();

    boolean isCloseIdleReaders();

    boolean isSkipSnapshotBackfill();

    boolean isScanNewlyAddedTableEnabled();

    boolean isAssignUnboundedChunkFirst();

    /** Factory for the {@code SourceConfig}. */
    @FunctionalInterface
    interface Factory<C extends SourceConfig> extends Serializable {

        C create(int subtask);
    }
}
