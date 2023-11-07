/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.operators.schema;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.graph.StreamGraphHasherV2;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.apache.flink.shaded.guava30.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava30.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava30.com.google.common.hash.Hashing;

import com.ververica.cdc.common.event.Event;

import java.nio.charset.StandardCharsets;

/** Factory to create {@link SchemaOperator}. */
public class SchemaOperatorFactory extends SimpleOperatorFactory<Event>
        implements CoordinatedOperatorFactory<Event>, OneInputStreamOperatorFactory<Event, Event> {

    private static final long serialVersionUID = 1L;

    /**
     * The transformation uid of schema operator.
     *
     * <p>Note: we use a deterministic operator id to identify the coordinator for cross-operator
     * communication.
     *
     * @see org.apache.flink.api.dag.Transformation#setUid(String)
     */
    public static final String SCHEMA_OPERATOR_UID = "$$_schema_operator_$$";

    /**
     * The operator id of schema operator and coordinator.
     *
     * <p>Note: we use a deterministic operator id to identify the coordinator for cross-operator
     * communication.
     */
    public static final OperatorID SCHEMA_EVOLUTION_OPERATOR_ID =
            new OperatorID(generateOperatorHash());

    /** This follows the operator hash generation logic for uid in {@link StreamGraphHasherV2}. */
    private static byte[] generateOperatorHash() {
        final HashFunction hashFunction = Hashing.murmur3_128(0);
        Hasher hasher = hashFunction.newHasher();
        hasher.putString(SCHEMA_OPERATOR_UID, StandardCharsets.UTF_8);
        return hasher.hash().asBytes();
    }

    protected SchemaOperatorFactory(StreamOperator<Event> operator) {
        super(operator);
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(String s, OperatorID operatorID) {
        return null;
    }
}
