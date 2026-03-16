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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.legacy.ParallelSourceFunction;

/**
 * Compatibility adapter for Flink 2.2. This class is part of the multi-version compatibility layer
 * that allows Flink CDC to work across different Flink versions.
 */
@Deprecated
@Internal
public abstract class RichParallelSourceFunction<OUT> extends AbstractRichFunction
        implements ParallelSourceFunction<OUT>, SourceFunction<OUT> {
    private static final long serialVersionUID = 1L;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        open(new Configuration());
    }

    public void open(Configuration parameters) throws Exception {}

    @Override
    public void run(
            org.apache.flink.streaming.api.functions.source.legacy.SourceFunction.SourceContext<OUT>
                    sourceContext)
            throws Exception {
        // Delegate to the new SourceFunction.run method
        SourceFunction.super.run(sourceContext);
    }
}
