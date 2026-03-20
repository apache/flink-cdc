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

package org.apache.flink.cdc.connectors.hudi.sink.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hudi.sink.bucket.BucketStreamWriteFunction;

/**
 * Extended version of {@link BucketStreamWriteFunction} that exposes a public setter for the
 * checkpoint ID.
 *
 * <p>This class is necessary because the parent class's {@code checkpointId} field is protected and
 * inaccessible from composition-based multi-table write functions. In a multi-table CDC sink, each
 * table requires its own write function instance, and these instances must be updated with the
 * current checkpoint ID for proper coordinator communication during checkpointing.
 *
 * <p>The public {@link #setCheckpointId(long)} method provides a clean API for parent write
 * functions to update the checkpoint ID without resorting to reflection-based access.
 *
 * @see BucketStreamWriteFunction
 * @see MultiTableEventStreamWriteFunction
 */
public class ExtendedBucketStreamWriteFunction extends BucketStreamWriteFunction {

    public ExtendedBucketStreamWriteFunction(Configuration config, RowType rowType) {
        super(config, rowType);
    }

    /**
     * Sets the checkpoint ID for this write function.
     *
     * <p>This method provides public access to update the protected {@code checkpointId} field
     * inherited from the parent class. The checkpoint ID is required for the write function to
     * properly communicate with the coordinator during checkpoint operations.
     *
     * @param checkpointId the checkpoint ID to set
     */
    public void setCheckpointId(long checkpointId) {
        this.checkpointId = checkpointId;
    }
}
