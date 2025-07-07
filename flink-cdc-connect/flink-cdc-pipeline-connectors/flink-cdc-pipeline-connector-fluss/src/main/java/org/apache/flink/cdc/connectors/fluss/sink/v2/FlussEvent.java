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

package org.apache.flink.cdc.connectors.fluss.sink.v2;

import com.alibaba.fluss.metadata.TablePath;

import java.util.List;

/** FlussEvent is a wrapper for a list of genericRows. */
public class FlussEvent {
    private final TablePath tablePath;
    private final List<FlussRowWithOp> rowWithOps;
    // if true, means that table schema has changed right before this genericRow.
    boolean shouldRefreshSchema;

    public FlussEvent(
            TablePath tablePath, List<FlussRowWithOp> rowWithOps, boolean shouldRefreshSchema) {
        this.tablePath = tablePath;
        this.rowWithOps = rowWithOps;
        this.shouldRefreshSchema = shouldRefreshSchema;
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public List<FlussRowWithOp> getRowWithOps() {
        return rowWithOps;
    }

    public boolean isShouldRefreshSchema() {
        return shouldRefreshSchema;
    }
}
