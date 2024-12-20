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

package org.apache.flink.cdc.composer.testsource.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

/** Configurations for {@link DistributedDataSource}. */
public class DistributedSourceOptions {
    public static final ConfigOption<Integer> TABLE_COUNT =
            ConfigOptions.key("table-count")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Number of parallelized tables in one single parallelism.");

    public static final ConfigOption<Boolean> DISTRIBUTED_TABLES =
            ConfigOptions.key("distributed-tables")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether this source should emit distributed tables that might present in multiple partitions. Defaults to false.");
}
