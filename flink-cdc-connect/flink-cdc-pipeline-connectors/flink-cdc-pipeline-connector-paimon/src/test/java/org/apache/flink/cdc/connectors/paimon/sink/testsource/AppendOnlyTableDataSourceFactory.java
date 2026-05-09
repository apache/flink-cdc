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

package org.apache.flink.cdc.connectors.paimon.sink.testsource;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceFunctionProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;

import java.util.HashSet;
import java.util.Set;

/** A source {@link Factory} to create {@link AppendOnlyTableDataSource}. */
@Internal
public class AppendOnlyTableDataSourceFactory implements DataSourceFactory {

    public static final String IDENTIFIER = "append-only-table-source";

    @Override
    public DataSource createDataSource(Context context) {
        return new AppendOnlyTableDataSource();
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
        return new HashSet<>();
    }

    /** A {@link DataSource} that emits append-only table data records. */
    @Internal
    private static class AppendOnlyTableDataSource implements DataSource {

        @Override
        public EventSourceProvider getEventSourceProvider() {
            return FlinkSourceFunctionProvider.of(new AppendOnlyTableSourceFunction());
        }

        @Override
        public MetadataAccessor getMetadataAccessor() {
            return null;
        }
    }
}
