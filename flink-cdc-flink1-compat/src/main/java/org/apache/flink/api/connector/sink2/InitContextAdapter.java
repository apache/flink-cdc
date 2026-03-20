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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.util.OptionalLong;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Internal
public class InitContextAdapter implements Sink.InitContext {
    private final WriterInitContext context;

    public InitContextAdapter(WriterInitContext context) {
        this.context = context;
    }

    public UserCodeClassLoader getUserCodeClassLoader() {
        return this.context.getUserCodeClassLoader();
    }

    public MailboxExecutor getMailboxExecutor() {
        return this.context.getMailboxExecutor();
    }

    public ProcessingTimeService getProcessingTimeService() {
        return this.context.getProcessingTimeService();
    }

    public int getSubtaskId() {
        return this.context.getTaskInfo().getIndexOfThisSubtask();
    }

    public int getNumberOfParallelSubtasks() {
        return this.context.getTaskInfo().getNumberOfParallelSubtasks();
    }

    public SinkWriterMetricGroup metricGroup() {
        return this.context.metricGroup();
    }

    public OptionalLong getRestoredCheckpointId() {
        return this.context.getRestoredCheckpointId();
    }

    @Override
    public JobInfo getJobInfo() {
        return this.context.getJobInfo();
    }

    @Override
    public TaskInfo getTaskInfo() {
        return this.context.getTaskInfo();
    }

    public SerializationSchema.InitializationContext asSerializationSchemaInitializationContext() {
        return this.context.asSerializationSchemaInitializationContext();
    }

    @Override
    public boolean isObjectReuseEnabled() {
        return this.context.isObjectReuseEnabled();
    }

    @Override
    public <IN> TypeSerializer<IN> createInputSerializer() {
        return this.context.createInputSerializer();
    }
}
