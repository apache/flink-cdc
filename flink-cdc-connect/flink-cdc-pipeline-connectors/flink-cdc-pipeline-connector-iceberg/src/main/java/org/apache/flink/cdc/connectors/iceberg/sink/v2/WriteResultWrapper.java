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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.cdc.common.event.TableId;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.WriteResult;

import java.io.Serializable;

/** A wrapper class for {@link WriteResult} and {@link TableId}. */
public class WriteResultWrapper implements Serializable {

    private static final long serialVersionUID = 1L;

    private final WriteResult writeResult;

    private final TableId tableId;

    public WriteResultWrapper(WriteResult writeResult, TableId tableId) {
        this.writeResult = writeResult;
        this.tableId = tableId;
    }

    public WriteResult getWriteResult() {
        return writeResult;
    }

    public TableId getTableId() {
        return tableId;
    }

    /** Build a simple description for the write result. */
    public String buildDescription() {
        long addCount = 0;
        if (writeResult.dataFiles() != null) {
            for (DataFile dataFile : writeResult.dataFiles()) {
                addCount += dataFile.recordCount();
            }
        }
        long deleteCount = 0;
        if (writeResult.deleteFiles() != null) {
            for (DeleteFile dataFile : writeResult.deleteFiles()) {
                deleteCount += dataFile.recordCount();
            }
        }
        return "WriteResult of "
                + tableId
                + ", AddCount: "
                + addCount
                + ", DeleteCount: "
                + deleteCount;
    }
}
