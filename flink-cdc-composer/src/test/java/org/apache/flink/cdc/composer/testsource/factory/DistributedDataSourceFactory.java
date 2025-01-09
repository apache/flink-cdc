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

package org.apache.flink.cdc.composer.testsource.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.composer.testsource.source.DistributedDataSource;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.composer.testsource.source.DistributedSourceOptions.DISTRIBUTED_TABLES;
import static org.apache.flink.cdc.composer.testsource.source.DistributedSourceOptions.TABLE_COUNT;

/** A source {@link Factory} to create {@link DistributedDataSource}. */
@Internal
public class DistributedDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "dds";

    @Override
    public DataSource createDataSource(Context context) {
        int tableCount = context.getFactoryConfiguration().get(TABLE_COUNT);
        boolean distributedTables = context.getFactoryConfiguration().get(DISTRIBUTED_TABLES);
        return new DistributedDataSource(tableCount, distributedTables);
    }

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return ImmutableSet.of(TABLE_COUNT, DISTRIBUTED_TABLES);
    }
}
