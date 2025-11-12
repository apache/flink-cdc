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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.IncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.MockPostgresDialect;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Properties;

/** Mock {@link PostgreSQLTableSource}. */
public class MockPostgreSQLTableSource extends PostgreSQLTableSource {
    public MockPostgreSQLTableSource(PostgreSQLTableSource postgreSQLTableSource) {
        super(
                (ResolvedSchema) get(postgreSQLTableSource, "physicalSchema"),
                (int) get(postgreSQLTableSource, "port"),
                (String) get(postgreSQLTableSource, "hostname"),
                (String) get(postgreSQLTableSource, "database"),
                (String) get(postgreSQLTableSource, "schemaName"),
                (String) get(postgreSQLTableSource, "tableName"),
                (String) get(postgreSQLTableSource, "username"),
                (String) get(postgreSQLTableSource, "password"),
                (String) get(postgreSQLTableSource, "pluginName"),
                (String) get(postgreSQLTableSource, "slotName"),
                (DebeziumChangelogMode) get(postgreSQLTableSource, "changelogMode"),
                (Properties) get(postgreSQLTableSource, "dbzProperties"),
                (boolean) get(postgreSQLTableSource, "enableParallelRead"),
                (int) get(postgreSQLTableSource, "splitSize"),
                (int) get(postgreSQLTableSource, "splitMetaGroupSize"),
                (int) get(postgreSQLTableSource, "fetchSize"),
                (Duration) get(postgreSQLTableSource, "connectTimeout"),
                (int) get(postgreSQLTableSource, "connectMaxRetries"),
                (int) get(postgreSQLTableSource, "connectionPoolSize"),
                (double) get(postgreSQLTableSource, "distributionFactorUpper"),
                (double) get(postgreSQLTableSource, "distributionFactorLower"),
                (Duration) get(postgreSQLTableSource, "heartbeatInterval"),
                (StartupOptions) get(postgreSQLTableSource, "startupOptions"),
                (String) get(postgreSQLTableSource, "chunkKeyColumn"),
                (boolean) get(postgreSQLTableSource, "closeIdleReaders"),
                (boolean) get(postgreSQLTableSource, "skipSnapshotBackfill"),
                (boolean) get(postgreSQLTableSource, "scanNewlyAddedTableEnabled"),
                (int) get(postgreSQLTableSource, "lsnCommitCheckpointsDelay"),
                (boolean) get(postgreSQLTableSource, "assignUnboundedChunkFirst"),
                (boolean) get(postgreSQLTableSource, "appendOnly"),
                (boolean) get(postgreSQLTableSource, "includePartitionedTables"));
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        ScanRuntimeProvider scanRuntimeProvider = super.getScanRuntimeProvider(scanContext);

        if (scanRuntimeProvider instanceof SourceProvider) {
            Source<RowData, ?, ?> source = ((SourceProvider) scanRuntimeProvider).createSource();
            Preconditions.checkState(
                    source instanceof PostgresSourceBuilder.PostgresIncrementalSource);

            PostgresSourceBuilder.PostgresIncrementalSource incrementalSource =
                    (PostgresSourceBuilder.PostgresIncrementalSource) source;

            try {
                Field configFactoryField =
                        IncrementalSource.class.getDeclaredField("configFactory");
                configFactoryField.setAccessible(true);
                PostgresSourceConfigFactory configFactory =
                        (PostgresSourceConfigFactory) configFactoryField.get(incrementalSource);
                MockPostgresDialect mockPostgresDialect =
                        new MockPostgresDialect(configFactory.create(0));

                Field dataSourceDialectField =
                        IncrementalSource.class.getDeclaredField("dataSourceDialect");
                dataSourceDialectField.setAccessible(true);
                dataSourceDialectField.set(incrementalSource, mockPostgresDialect);
            } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
                throw new FlinkRuntimeException(e);
            }
        }

        return scanRuntimeProvider;
    }

    private static Object get(PostgreSQLTableSource postgreSQLTableSource, String name) {
        try {
            Field field = postgreSQLTableSource.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(postgreSQLTableSource);
        } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
            throw new FlinkRuntimeException(e);
        }
    }
}
