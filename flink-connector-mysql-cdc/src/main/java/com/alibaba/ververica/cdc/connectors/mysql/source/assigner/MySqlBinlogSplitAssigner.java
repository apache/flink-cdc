/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.assigner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import static com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator.BINLOG_SPLIT_ID;
import static com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset.getCurrentBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.SplitKeyUtils.validateAndGetSplitKeyType;

/**
 * A split assigner that assign the binlog split for all captured tables.
 *
 * <p>This assigner is used for latest-offset startup mode.
 */
public class MySqlBinlogSplitAssigner extends MySqlSplitAssigner {

    private RowType splitKeyType;

    public MySqlBinlogSplitAssigner(
            Configuration configuration,
            RowType definedPkType,
            Collection<TableId> alreadyProcessedTables,
            Collection<MySqlSplit> remainingSplits) {
        super(configuration, definedPkType, alreadyProcessedTables, remainingSplits);
    }

    /**
     * Gets the binlog split for latest-offset.
     *
     * <p>When this method returns an empty {@code Optional} when the unique binlog split has
     * returned.
     */
    public Optional<MySqlSplit> getNext() {
        if (!remainingSplits.isEmpty()) {
            MySqlSplit split = remainingSplits.iterator().next();
            remainingSplits.remove(split);
            return Optional.of(split);
        } else {
            if (alreadyProcessedTables.isEmpty()) {
                for (TableId tableId : capturedTables) {
                    final TableChanges.TableChange tableChange = getTableSchema(tableId);
                    this.tableSchemas.put(tableId, tableChange);
                    this.splitKeyType =
                            validateAndGetSplitKeyType(definedPkType, tableChange.getTable());
                    this.alreadyProcessedTables.add(tableId);
                }
                BinlogOffset startingOffset = getCurrentBinlogPosition(this.jdbc);
                remainingSplits.add(
                        new MySqlBinlogSplit(
                                BINLOG_SPLIT_ID,
                                splitKeyType,
                                startingOffset,
                                BinlogOffset.NO_STOPPING_OFFSET,
                                new ArrayList<>(),
                                tableSchemas));
                return getNext();
            } else {
                return Optional.empty();
            }
        }
    }
}
