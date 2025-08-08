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

package org.apache.flink.cdc.connectors.hudi.sink.event;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;

/**
 * A serializer interface for converting input records into {@link HoodieFlinkInternalRow} for Hudi
 * writing.
 *
 * @param <T> The input record type to be serialized
 */
public interface HudiRecordSerializer<T> {

    /**
     * Serialize an input record into HoodieFlinkInternalRow.
     *
     * @param record The input record to serialize
     * @param fileId The file ID to assign to the record
     * @param instantTime The instant time to assign to the record
     * @return HoodieFlinkInternalRow or null if the record doesn't produce a data record
     */
    HoodieFlinkInternalRow serialize(T record, String fileId, String instantTime);

    /**
     * Serialize an input record into HoodieFlinkInternalRow without fileId and instantTime. The
     * fileId and instantTime will be set later by the caller.
     *
     * @param record The input record to serialize
     * @return HoodieFlinkInternalRow or null if the record doesn't produce a data record
     */
    HoodieFlinkInternalRow serialize(T record);
}
