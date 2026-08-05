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

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;

import javax.annotation.Nullable;

/** A {@link HashFunctionProvider} that preserves the upstream subtask distribution. */
@Internal
public class ForwardHashFunctionProvider implements HashFunctionProvider<DataChangeEvent> {

    private static final long serialVersionUID = 1L;

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new ForwardHashFunction();
    }

    private static class ForwardHashFunction implements HashFunction<DataChangeEvent> {

        @Override
        public int hashcode(DataChangeEvent event) {
            return 0;
        }

        @Override
        public int hashcode(int sourceIndex, DataChangeEvent event) {
            return sourceIndex;
        }
    }
}
