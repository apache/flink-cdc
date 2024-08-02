/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * {@code Elasticsearch8AsyncSinkSerializer} is used to serialize and deserialize {@link Operation}
 * objects for Elasticsearch 8 async sink.
 */
public class Elasticsearch8AsyncSinkSerializer extends AsyncSinkWriterStateSerializer<Operation> {

    /**
     * Serializes the given {@link Operation} to the provided {@link DataOutputStream}.
     *
     * @param request the Operation to serialize.
     * @param out the DataOutputStream to which the Operation will be serialized.
     */
    @Override
    protected void serializeRequestToStream(Operation request, DataOutputStream out) {
        new OperationSerializer().serialize(request, out);
    }

    /**
     * Deserializes an {@link Operation} from the provided {@link DataInputStream}.
     *
     * @param requestSize the size of the request in bytes.
     * @param in the DataInputStream from which the Operation will be deserialized.
     * @return the deserialized Operation.
     */
    @Override
    protected Operation deserializeRequestFromStream(long requestSize, DataInputStream in) {
        return new OperationSerializer().deserialize(requestSize, in);
    }

    /**
     * Returns the version of this serializer.
     *
     * @return the version number.
     */
    @Override
    public int getVersion() {
        return 1;
    }
}
