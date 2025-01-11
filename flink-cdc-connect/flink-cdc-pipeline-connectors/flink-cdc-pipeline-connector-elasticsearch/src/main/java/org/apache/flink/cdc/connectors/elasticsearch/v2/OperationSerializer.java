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

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * OperationSerializer is responsible for the serialization and deserialization of an {@link
 * Operation}.
 */
public class OperationSerializer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationSerializer.class);

    private final Kryo kryo = new Kryo();

    public OperationSerializer() {
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    }

    /**
     * Serializes the given {@link Operation} to the provided {@link DataOutputStream}.
     *
     * @param request the Operation to serialize
     * @param out the DataOutputStream to which the Operation will be serialized
     */
    public void serialize(Operation request, DataOutputStream out) {
        try (Output output = new Output(out)) {
            kryo.writeObjectOrNull(output, request, Operation.class);
            output.flush();
        }
    }

    /**
     * Deserializes an {@link Operation} from the provided {@link DataInputStream}.
     *
     * @param requestSize the size of the request in bytes
     * @param in the DataInputStream from which the Operation will be deserialized
     * @return the deserialized Operation, or null if deserialization fails
     */
    public Operation deserialize(long requestSize, DataInputStream in) {
        try (Input input = new Input(in, (int) requestSize)) {
            if (input.available() > 0) {
                return kryo.readObject(input, Operation.class);
            } else {
                return null; // Skip if input stream is empty
            }
        } catch (Exception e) {
            LOG.error("Failed to deserialize Operation: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Calculates the serialized size of the given {@link Operation}.
     *
     * @param operation the Operation whose size is to be calculated
     * @return the size of the serialized Operation in bytes
     */
    public int size(Operation operation) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeObjectOrNull(output, operation, Operation.class);
            output.flush();
            return byteArrayOutputStream.size();
        }
    }
}
