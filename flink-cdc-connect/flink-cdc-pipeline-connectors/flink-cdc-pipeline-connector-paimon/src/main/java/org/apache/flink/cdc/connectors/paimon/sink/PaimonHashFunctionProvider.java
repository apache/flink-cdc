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

package org.apache.flink.cdc.connectors.paimon.sink;

import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.function.HashFunction;
import org.apache.flink.cdc.common.function.HashFunctionProvider;
import org.apache.flink.cdc.common.schema.Schema;

import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.time.ZoneId;

/** A {@link HashFunctionProvider} implementation for {@link PaimonDataSink}. */
public class PaimonHashFunctionProvider implements HashFunctionProvider<DataChangeEvent> {

    private final Options options;

    private final ZoneId zoneId;

    private final int parallelism;

    public PaimonHashFunctionProvider(Options options, ZoneId zoneId, int parallelism) {
        this.options = options;
        this.zoneId = zoneId;
        this.parallelism = parallelism;
    }

    @Override
    public HashFunction<DataChangeEvent> getHashFunction(@Nullable TableId tableId, Schema schema) {
        return new PaimonHashFunction(options, tableId, schema, zoneId, parallelism);
    }
}
