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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** A {@link IcebergWriterState} for Apache Iceberg. */
public class IcebergWriterState {
    public static final int VERSION = 0;

    public static final ObjectMapper MAPPER = new ObjectMapper();

    private final Long checkpointId;

    @JsonCreator
    public IcebergWriterState(@JsonProperty("checkpointId") Long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public Long getCheckpointId() {
        return checkpointId;
    }

    public byte[] toBytes() throws IOException {
        return MAPPER.writeValueAsBytes(this);
    }

    public static IcebergWriterState fromBytes(byte[] bytes) throws IOException {
        return MAPPER.readValue(bytes, IcebergWriterState.class);
    }

    @Override
    public String toString() {
        return "IcebergWriterState{" + "checkpointId=" + checkpointId + '}';
    }
}
