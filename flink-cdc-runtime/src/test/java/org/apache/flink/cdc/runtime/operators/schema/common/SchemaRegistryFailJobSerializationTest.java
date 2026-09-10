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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.pipeline.RouteMode;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.sink.MetadataApplier;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.SerializedThrowable;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for the operator-coordinator RPC exception-serialization bug.
 *
 * <p>When {@link SchemaRegistry#failJob} completes a coordination response exceptionally, the
 * exception crosses the operator-coordinator RPC boundary and is deserialized by flink-rpc-akka
 * using an isolated classloader. If the exception's cause chain contains a class that only lives in
 * the user classloader (e.g. {@code com.mysql.cj.exceptions.ConnectionIsClosedException} raised
 * during table discovery), plain Java serialization fails on the receiving side with {@link
 * ClassNotFoundException} and the response is lost, stalling the request until rpcTimeout and
 * failing with a misleading {@code TimeoutException}. {@code failJob} therefore wraps the exception
 * into a {@link SerializedThrowable} so it survives that boundary.
 */
class SchemaRegistryFailJobSerializationTest {

    /** Stands in for a MySQL driver exception that only lives in the user classloader. */
    private static final class UserClassloaderOnlyException extends RuntimeException {
        UserClassloaderOnlyException(String message) {
            super(message);
        }
    }

    @Test
    void failJobWrapsExceptionIntoSerializedThrowable() throws Exception {
        MockedOperatorCoordinatorContext context =
                new MockedOperatorCoordinatorContext(new OperatorID(), getClass().getClassLoader());
        try (TestSchemaRegistry registry = new TestSchemaRegistry(context)) {
            Throwable driverEx =
                    new UserClassloaderOnlyException("connection was unexpectedly lost");
            RuntimeException wrapped =
                    new RuntimeException("Failed to discovery tables to capture", driverEx);

            registry.failJobForTest("table discovery", wrapped);

            assertThat(context.getFailureCause()).isInstanceOf(SerializedThrowable.class);
        }
    }

    @Test
    void wrappedFailureSurvivesIsolatedClassloaderButRawOneDoesNot() throws Exception {
        Throwable driverEx = new UserClassloaderOnlyException("connection was unexpectedly lost");
        RuntimeException rawWrapped =
                new RuntimeException("Failed to discovery tables to capture", driverEx);

        // The isolated classloader (as flink-rpc-akka uses) cannot see the user-jar exception.
        IsolatedClassLoader isolated =
                new IsolatedClassLoader(
                        getClass().getClassLoader(), UserClassloaderOnlyException.class.getName());

        // Before the fix: the raw exception chain fails to deserialize -> response is lost.
        byte[] rawBytes = serialize(rawWrapped);
        assertThatThrownBy(() -> deserializeWith(rawBytes, isolated))
                .isInstanceOf(ClassNotFoundException.class)
                .hasMessageContaining(UserClassloaderOnlyException.class.getName());

        // After the fix: SerializedThrowable survives and preserves the real cause as text.
        MockedOperatorCoordinatorContext context =
                new MockedOperatorCoordinatorContext(new OperatorID(), getClass().getClassLoader());
        try (TestSchemaRegistry registry = new TestSchemaRegistry(context)) {
            registry.failJobForTest("table discovery", rawWrapped);
            SerializedThrowable serialized = (SerializedThrowable) context.getFailureCause();

            byte[] wrappedBytes = serialize(serialized);
            Object result = deserializeWith(wrappedBytes, isolated);

            assertThat(result).isInstanceOf(SerializedThrowable.class);
            assertThat(((SerializedThrowable) result).getFullStringifiedStackTrace())
                    .contains("Failed to discovery tables to capture")
                    .contains("connection was unexpectedly lost");
        }
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    /** Minimal concrete {@link SchemaRegistry} that only exposes {@code failJob} for testing. */
    private static final class TestSchemaRegistry extends SchemaRegistry {
        private final ExecutorService executor;

        TestSchemaRegistry(OperatorCoordinator.Context context) {
            this(context, Executors.newSingleThreadExecutor());
        }

        private TestSchemaRegistry(OperatorCoordinator.Context context, ExecutorService executor) {
            super(
                    context,
                    "test-registry",
                    executor,
                    new NoOpMetadataApplier(),
                    Collections.emptyList(),
                    RouteMode.ALL_MATCH,
                    SchemaChangeBehavior.EVOLVE,
                    Duration.ofSeconds(30));
            this.executor = executor;
        }

        void failJobForTest(String taskDescription, Throwable t) {
            failJob(taskDescription, t);
        }

        @Override
        protected void snapshot(CompletableFuture<byte[]> resultFuture) {
            resultFuture.complete(new byte[0]);
        }

        @Override
        protected void restore(byte[] checkpointData) {}

        @Override
        protected void handleFlushSuccessEvent(FlushSuccessEvent event) {}

        @Override
        protected void handleCustomCoordinationRequest(
                CoordinationRequest request,
                CompletableFuture<CoordinationResponse> responseFuture) {}

        @Override
        public void close() throws Exception {
            executor.shutdownNow();
        }
    }

    private static final class NoOpMetadataApplier implements MetadataApplier {
        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent) {}
    }

    /** Simulates flink-rpc-akka's isolated classloader that cannot see user-jar classes. */
    private static final class IsolatedClassLoader extends ClassLoader {
        private final String hiddenClassName;

        IsolatedClassLoader(ClassLoader parent, String hiddenClassName) {
            super(parent);
            this.hiddenClassName = hiddenClassName;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.equals(hiddenClassName)) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name);
        }
    }

    private static final class ClassLoaderObjectInputStream extends ObjectInputStream {
        private final ClassLoader classLoader;

        ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            return Class.forName(desc.getName(), false, classLoader);
        }
    }

    private static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(o);
        }
        return bytes.toByteArray();
    }

    private static Object deserializeWith(byte[] data, ClassLoader classLoader) throws Exception {
        try (ClassLoaderObjectInputStream in =
                new ClassLoaderObjectInputStream(new ByteArrayInputStream(data), classLoader)) {
            return in.readObject();
        }
    }
}
