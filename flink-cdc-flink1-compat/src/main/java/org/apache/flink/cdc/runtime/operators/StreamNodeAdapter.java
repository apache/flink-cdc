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

package org.apache.flink.cdc.runtime.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/** Provides the operator-factory mutation missing from Flink 1.20's StreamNode API. */
@Internal
public final class StreamNodeAdapter {
    private static final Field OPERATOR_FACTORY = operatorFactoryField();

    private StreamNodeAdapter() {}

    public static void setOperatorFactory(StreamNode node, StreamOperatorFactory<?> factory) {
        try {
            OPERATOR_FACTORY.set(node, factory);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot adapt the Flink 1.20 operator factory", e);
        }
    }

    private static Field operatorFactoryField() {
        try {
            Field field = StreamNode.class.getDeclaredField("operatorFactory");
            if (field.getType() != StreamOperatorFactory.class
                    || Modifier.isFinal(field.getModifiers())
                    || Modifier.isStatic(field.getModifiers())
                    || !field.trySetAccessible()) {
                throw new IllegalStateException("Unsupported Flink 1.20 StreamNode layout");
            }
            return field;
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("Unsupported Flink 1.20 StreamNode layout", e);
        }
    }
}
