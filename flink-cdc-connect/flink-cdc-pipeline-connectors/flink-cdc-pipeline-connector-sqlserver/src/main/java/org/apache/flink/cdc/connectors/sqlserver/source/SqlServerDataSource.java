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

package org.apache.flink.cdc.connectors.sqlserver.source;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.annotation.VisibleForTesting;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfig;
import org.apache.flink.cdc.connectors.sqlserver.source.config.SqlServerSourceConfigFactory;
import org.apache.flink.cdc.connectors.sqlserver.source.dialect.SqlServerDialect;
import org.apache.flink.cdc.connectors.sqlserver.source.offset.LsnFactory;
import org.apache.flink.cdc.connectors.sqlserver.table.SqlServerReadableMetadata;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import java.util.ArrayList;
import java.util.List;

/** A {@link DataSource} for SQL Server CDC connector. */
@Internal
public class SqlServerDataSource implements DataSource {

    private final SqlServerSourceConfigFactory configFactory;
    private final SqlServerSourceConfig sqlServerSourceConfig;

    private final List<SqlServerReadableMetadata> readableMetadataList;

    public SqlServerDataSource(SqlServerSourceConfigFactory configFactory) {
        this(configFactory, new ArrayList<>());
    }

    public SqlServerDataSource(
            SqlServerSourceConfigFactory configFactory,
            List<SqlServerReadableMetadata> readableMetadataList) {
        this.configFactory = configFactory;
        this.sqlServerSourceConfig = configFactory.create(0);
        this.readableMetadataList = readableMetadataList;
    }

    @Override
    public EventSourceProvider getEventSourceProvider() {
        SqlServerEventDeserializer deserializer =
                new SqlServerEventDeserializer(
                        DebeziumChangelogMode.ALL,
                        sqlServerSourceConfig.isIncludeSchemaChanges(),
                        readableMetadataList);

        LsnFactory lsnFactory = new LsnFactory(sqlServerSourceConfig);
        SqlServerDialect sqlServerDialect = new SqlServerDialect(sqlServerSourceConfig);

        SqlServerPipelineSource source =
                new SqlServerPipelineSource(
                        configFactory, deserializer, lsnFactory, sqlServerDialect);

        return FlinkSourceProvider.of(source);
    }

    @Override
    public MetadataAccessor getMetadataAccessor() {
        return new SqlServerMetadataAccessor(sqlServerSourceConfig);
    }

    @Override
    public boolean isParallelMetadataSource() {
        // During incremental stage, SQL Server never emits schema change events on different
        // partitions (since it has one transaction log stream only.)
        return false;
    }

    @VisibleForTesting
    public SqlServerSourceConfig getSqlServerSourceConfig() {
        return sqlServerSourceConfig;
    }
}
