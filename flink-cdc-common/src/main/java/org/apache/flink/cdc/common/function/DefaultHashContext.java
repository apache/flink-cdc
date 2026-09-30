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

package org.apache.flink.cdc.common.function;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.utils.Preconditions;

/** Default implementation of {@link HashFunction.HashContext}. */
@Internal
public final class DefaultHashContext implements HashFunction.HashContext {

    private final int sourceSubtaskIndex;
    private final int downstreamParallelism;

    public DefaultHashContext(int sourceSubtaskIndex, int downstreamParallelism) {
        Preconditions.checkArgument(
                sourceSubtaskIndex >= 0,
                "sourceSubtaskIndex must be greater than or equal to 0, but was %s.",
                sourceSubtaskIndex);
        Preconditions.checkArgument(
                downstreamParallelism > 0,
                "downstreamParallelism must be greater than 0, but was %s.",
                downstreamParallelism);
        this.sourceSubtaskIndex = sourceSubtaskIndex;
        this.downstreamParallelism = downstreamParallelism;
    }

    @Override
    public int getSourceSubtaskIndex() {
        return sourceSubtaskIndex;
    }

    @Override
    public int getDownstreamParallelism() {
        return downstreamParallelism;
    }
}
