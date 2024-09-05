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

package org.apache.flink.cdc.runtime.operators.schema.coordinator;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.route.RouteRule;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.FatalExitExceptionHandler;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/** Provider of {@link SchemaRegistry}. */
@Internal
public class SchemaRegistryProvider implements OperatorCoordinator.Provider {
    private static final long serialVersionUID = 1L;

    private final OperatorID operatorID;
    private final String operatorName;
    private final MetadataApplier metadataApplier;
    private final List<RouteRule> routingRules;
    private final SchemaChangeBehavior schemaChangeBehavior;

    public SchemaRegistryProvider(
            OperatorID operatorID,
            String operatorName,
            MetadataApplier metadataApplier,
            List<RouteRule> routingRules,
            SchemaChangeBehavior schemaChangeBehavior) {
        this.operatorID = operatorID;
        this.operatorName = operatorName;
        this.metadataApplier = metadataApplier;
        this.routingRules = routingRules;
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    @Override
    public OperatorID getOperatorId() {
        return operatorID;
    }

    @Override
    public OperatorCoordinator create(OperatorCoordinator.Context context) throws Exception {
        CoordinatorExecutorThreadFactory coordinatorThreadFactory =
                new CoordinatorExecutorThreadFactory(
                        "schema-evolution-coordinator", context.getUserCodeClassloader());
        ExecutorService coordinatorExecutor =
                Executors.newSingleThreadExecutor(coordinatorThreadFactory);
        return new SchemaRegistry(
                operatorName,
                context,
                coordinatorExecutor,
                metadataApplier,
                routingRules,
                schemaChangeBehavior);
    }

    /** A thread factory class that provides some helper methods. */
    public static class CoordinatorExecutorThreadFactory implements ThreadFactory {

        private final String coordinatorThreadName;
        private final ClassLoader cl;
        private final Thread.UncaughtExceptionHandler errorHandler;

        private Thread t;

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName, final ClassLoader contextClassLoader) {
            this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
        }

        CoordinatorExecutorThreadFactory(
                final String coordinatorThreadName,
                final ClassLoader contextClassLoader,
                final Thread.UncaughtExceptionHandler errorHandler) {
            this.coordinatorThreadName = coordinatorThreadName;
            this.cl = contextClassLoader;
            this.errorHandler = errorHandler;
        }

        @Override
        public synchronized Thread newThread(Runnable r) {
            if (t != null) {
                throw new Error(
                        "This indicates that a fatal error has happened and caused the "
                                + "coordinator executor thread to exit. Check the earlier logs"
                                + "to see the root cause of the problem.");
            }
            t = new Thread(r, coordinatorThreadName);
            t.setContextClassLoader(cl);
            t.setUncaughtExceptionHandler(errorHandler);
            return t;
        }
    }
}
