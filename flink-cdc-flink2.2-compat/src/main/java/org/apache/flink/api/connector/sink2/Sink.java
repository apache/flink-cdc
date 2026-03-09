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

package org.apache.flink.api.connector.sink2;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.Consumer;

/** Compatibility adapter to add class that was present in Flink 1.20 but removed in Flink 2.2. */
@Public
public interface Sink<InputT> extends Serializable {

    SinkWriter<InputT> createWriter(WriterInitContext var1) throws IOException;

    /**
     * @deprecated
     */
    @Deprecated
    SinkWriter<InputT> createWriter(InitContext var1) throws IOException;

    /**
     * @deprecated
     */
    @Deprecated
    @PublicEvolving
    public interface InitContext extends org.apache.flink.api.connector.sink2.InitContext {
        UserCodeClassLoader getUserCodeClassLoader();

        MailboxExecutor getMailboxExecutor();

        ProcessingTimeService getProcessingTimeService();

        SinkWriterMetricGroup metricGroup();

        SerializationSchema.InitializationContext asSerializationSchemaInitializationContext();

        boolean isObjectReuseEnabled();

        <IN> TypeSerializer<IN> createInputSerializer();

        @Experimental
        default <MetaT> Optional<Consumer<MetaT>> metadataConsumer() {
            return Optional.empty();
        }
    }
}
