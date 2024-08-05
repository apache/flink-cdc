/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.writer;

import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;
import org.apache.flink.cdc.connectors.maxcompute.utils.MaxComputeUtils;

import com.aliyun.odps.data.ArrayRecord;

import java.io.IOException;

/** the interface of all writer to write {@link ArrayRecord} to maxcompute. */
public interface MaxComputeWriter {

    static MaxComputeWriter batchWriter(
            MaxComputeOptions options,
            MaxComputeWriteOptions writeOptions,
            SessionIdentifier sessionIdentifier)
            throws IOException {
        if (MaxComputeUtils.isTransactionalTable(options, sessionIdentifier)) {
            return new BatchUpsertWriter(options, writeOptions, sessionIdentifier);
        } else {
            return new BatchAppendWriter(options, writeOptions, sessionIdentifier);
        }
    }

    SessionIdentifier getSessionIdentifier();

    ArrayRecord newElement();

    void write(ArrayRecord record) throws IOException;

    void delete(ArrayRecord record) throws IOException;

    void flush() throws IOException;

    void commit() throws IOException;

    String getId();
}
