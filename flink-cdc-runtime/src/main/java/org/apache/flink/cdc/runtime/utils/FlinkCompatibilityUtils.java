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

package org.apache.flink.cdc.runtime.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Utility class for handling Flink version compatibility issues. */
@Internal
public class FlinkCompatibilityUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCompatibilityUtils.class);

    private FlinkCompatibilityUtils() {
        // Private constructor to prevent instantiation
    }

    /**
     * Sets the chaining strategy for an operator via reflection.
     *
     * <p>This method is needed because Flink 2.x removed the {@code chainingStrategy} field from
     * {@link AbstractStreamOperator}. This method attempts to set the field via reflection for
     * backward compatibility with Flink 1.x, and silently ignores the failure in Flink 2.x.
     *
     * @param operator the operator to set the chaining strategy for
     * @param strategy the chaining strategy to set
     */
    public static void setChainingStrategyIfAvailable(
            AbstractStreamOperator<?> operator, ChainingStrategy strategy) {
        try {
            Field field = AbstractStreamOperator.class.getDeclaredField("chainingStrategy");
            field.setAccessible(true);
            field.set(operator, strategy);
            LOG.debug("Successfully set chainingStrategy to {} via reflection", strategy);
        } catch (NoSuchFieldException e) {
            LOG.debug(
                    "chainingStrategy field not available (likely Flink 2.x), skipping for operator {}",
                    operator.getClass().getSimpleName());
        } catch (Exception e) {
            LOG.warn(
                    "Failed to set chainingStrategy via reflection for operator {}",
                    operator.getClass().getSimpleName(),
                    e);
        }
    }

    /**
     * Resolves schema compatibility between a serializer snapshot and a new serializer, handling
     * API differences between Flink 1.x and 2.x.
     *
     * <p>In Flink 1.x, the method is {@code snapshot.resolveSchemaCompatibility(serializer)}. In
     * Flink 2.x, the method was changed to {@code
     * newSnapshot.resolveSchemaCompatibility(oldSnapshot)}.
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> snapshot, TypeSerializer<T> serializer) {
        // Try Flink 1.x approach: snapshot.resolveSchemaCompatibility(serializer)
        try {
            Method m =
                    snapshot.getClass()
                            .getMethod("resolveSchemaCompatibility", TypeSerializer.class);
            return (TypeSerializerSchemaCompatibility<T>) m.invoke(snapshot, serializer);
        } catch (NoSuchMethodException ignored) {
            // fall through to Flink 2.x approach
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(
                    "Failed to resolve schema compatibility (Flink 1.x path)", cause);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve schema compatibility (Flink 1.x path)", e);
        }

        // Flink 2.x approach: newSnapshot.resolveSchemaCompatibility(oldSnapshot)
        try {
            TypeSerializerSnapshot<T> newSnapshot = serializer.snapshotConfiguration();
            Method m =
                    newSnapshot
                            .getClass()
                            .getMethod("resolveSchemaCompatibility", TypeSerializerSnapshot.class);
            return (TypeSerializerSchemaCompatibility<T>) m.invoke(newSnapshot, snapshot);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(
                    "Failed to resolve schema compatibility (Flink 2.x path)", cause);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to resolve schema compatibility (Flink 2.x path)", e);
        }
    }

    /**
     * Calls {@link AbstractStreamOperator#setup} via reflection to handle access level changes
     * between Flink 1.x (public) and Flink 2.x (protected).
     */
    public static void setupOperator(
            AbstractStreamOperator<?> operator, Object containingTask, Object config, Object output)
            throws Exception {
        Method setupMethod = null;
        for (Method m : AbstractStreamOperator.class.getDeclaredMethods()) {
            if ("setup".equals(m.getName()) && m.getParameterCount() == 3) {
                setupMethod = m;
                break;
            }
        }
        if (setupMethod == null) {
            throw new NoSuchMethodException("Cannot find setup method in AbstractStreamOperator");
        }
        setupMethod.setAccessible(true);
        try {
            setupMethod.invoke(operator, containingTask, config, output);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }
}
