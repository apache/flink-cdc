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

package com.ververica.cdc.connectors.tdsql.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of {@link RecordsWithSplitIds} which contains the records of one table split.
 */
public class TdSqlRecords implements RecordsWithSplitIds<SourceRecord> {
    private final RecordsWithSplitIds<SourceRecord> mySqlRecords;
    private final TdSqlSet set;

    public TdSqlRecords(RecordsWithSplitIds<SourceRecord> mySqlRecords, TdSqlSet set) {
        this.mySqlRecords = mySqlRecords;
        this.set = set;
    }

    @Nullable
    @Override
    public String nextSplit() {
        String mysqlSplitId = mySqlRecords.nextSplit();
        if (mysqlSplitId == null) {
            return null;
        }
        return tdSqlSplitId(mysqlSplitId);
    }

    @Nullable
    @Override
    public SourceRecord nextRecordFromSplit() {
        return mySqlRecords.nextRecordFromSplit();
    }

    @Override
    public Set<String> finishedSplits() {
        Set<String> finishedSplits = mySqlRecords.finishedSplits();
        return finishedSplits.stream().map(this::tdSqlSplitId).collect(Collectors.toSet());
    }

    private String tdSqlSplitId(String mysqlSplitId) {
        return set.getSetKey() + ":" + mysqlSplitId;
    }
}
