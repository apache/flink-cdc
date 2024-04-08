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

package org.apache.flink.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import org.apache.flink.cdc.connectors.mysql.source.reader.MySqlSourceReader;
import org.apache.flink.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link MySqlSourceEnumerator} sends to {@link MySqlSourceReader} to
 * pass binlog metadata, i.e. {@link FinishedSnapshotSplitInfo}.
 */
public class BinlogSplitMetaEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String splitId;

    /** The metadata of binlog split is divided to multiple groups. */
    private final int metaGroupId;
    /**
     * The serialized metadata of binlog split, it's serialized/deserialize by {@link
     * FinishedSnapshotSplitInfo#serialize(FinishedSnapshotSplitInfo)} and {@link
     * FinishedSnapshotSplitInfo#deserialize(byte[])}.
     */
    private final List<byte[]> metaGroup;

    private final int totalFinishedSplitSize;

    public BinlogSplitMetaEvent(
            String splitId,
            int metaGroupId,
            @Nullable List<byte[]> metaGroup,
            int totalFinishedSplitSize) {
        this.splitId = splitId;
        this.metaGroupId = metaGroupId;
        this.metaGroup = metaGroup;
        this.totalFinishedSplitSize = totalFinishedSplitSize;
    }

    public String getSplitId() {
        return splitId;
    }

    public int getMetaGroupId() {
        return metaGroupId;
    }

    public List<byte[]> getMetaGroup() {
        return metaGroup;
    }

    public int getTotalFinishedSplitSize() {
        return totalFinishedSplitSize;
    }
}
