/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.postgres.source.assigners;

import com.ververica.cdc.connectors.base.config.JdbcSourceConfig;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.assigner.SnapshotSplitAssigner;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.postgres.PostgresTestBase;
import com.ververica.cdc.connectors.postgres.source.PostgresDialect;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfig;
import com.ververica.cdc.connectors.postgres.source.config.PostgresSourceConfigFactory;
import com.ververica.cdc.connectors.postgres.source.offset.PostgresOffsetFactory;
import com.ververica.cdc.connectors.postgres.testutils.UniqueDatabase;
import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

/** Unit tests for {@link SnapshotSplitAssigner}. */
public class PostgresSnapshotSplitAssignerTest extends PostgresTestBase {

    private static final String DB_NAME_PREFIX = "postgres";
    private static final String SCHEMA_NAME = "customer";

    private final UniqueDatabase customDatabase =
            new UniqueDatabase(
                    POSTGRES_CONTAINER,
                    DB_NAME_PREFIX,
                    SCHEMA_NAME,
                    POSTGRES_CONTAINER.getUsername(),
                    POSTGRES_CONTAINER.getPassword());

    @Test
    public void testSplitSnapshots() {
        List<SourceSplitBase> sourceSplitBases = getSnapshotSplits();
        List<Object[][]> actual =
                sourceSplitBases.stream()
                        .map(
                                split ->
                                        new Object[][] {
                                            split.asSnapshotSplit().getSplitStart(),
                                            split.asSnapshotSplit().getSplitEnd()
                                        })
                        .collect(Collectors.toList());
        List<Object[][]> expected = new LinkedList<>();
        expected.add(new Object[][] {null, new Object[] {1005}});
        expected.add(new Object[][] {new Object[] {1005}, new Object[] {1909}});
        expected.add(new Object[][] {new Object[] {1909}, null});
        expected.add(new Object[][] {null, new Object[] {1005}});
        expected.add(new Object[][] {new Object[] {1005}, new Object[] {1909}});
        expected.add(new Object[][] {new Object[] {1909}, null});
        assertArrayEquals(expected.toArray(new Object[0][0]), actual.toArray(new Object[0][0]));
    }

    private List<SourceSplitBase> getSnapshotSplits() {
        customDatabase.createAndInitialize();
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.hostname(customDatabase.getHost());
        configFactory.port(customDatabase.getDatabasePort());
        configFactory.username(customDatabase.getUsername());
        configFactory.password(customDatabase.getPassword());
        configFactory.database(customDatabase.getDatabaseName());
        configFactory.splitSize(10);
        configFactory.startupOptions(StartupOptions.initial());
        PostgresSourceConfig sourceConfig = configFactory.create(0);
        PostgresDialect dialect = new PostgresDialect(configFactory);
        List<TableId> remainingSplits = new ArrayList<>();
        remainingSplits.add(TableId.parse(SCHEMA_NAME + "." + "customers"));
        remainingSplits.add(TableId.parse(SCHEMA_NAME + "." + "customers_1"));
        PostgresOffsetFactory offsetFactory = new PostgresOffsetFactory();
        SnapshotSplitAssigner<JdbcSourceConfig> assigner =
                new SnapshotSplitAssigner<>(
                        sourceConfig, 1, remainingSplits, false, dialect, offsetFactory);
        assigner.open();
        List<SourceSplitBase> splits = new ArrayList<>();
        while (!assigner.noMoreSplits()) {
            Optional<SourceSplitBase> sourceSplitBase = assigner.getNext();
            sourceSplitBase.ifPresent(splits::add);
        }

        return splits;
    }
}
