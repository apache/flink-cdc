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

package org.apache.flink.cdc.common.model;

import org.apache.flink.cdc.common.annotation.Experimental;

import java.io.Serializable;

/**
 * Marker interface for a runtime AI model client. Concrete capabilities are declared via ability
 * interfaces in {@code org.apache.flink.cdc.common.model.abilities}.
 *
 * <p>Implementations must be {@link Serializable} so that they can be distributed across Flink task
 * managers together with the operator that holds them.
 */
@Experimental
public interface AiModelClient extends Serializable, AutoCloseable {

    default void open() throws Exception {
        // Do nothing
    }

    @Override
    default void close() throws Exception {
        // Do nothing
    }
}
