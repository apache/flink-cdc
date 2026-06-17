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

package org.apache.flink.cdc.connectors.dws.sink.v2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** A staged DWS table ready to be committed into the target table. */
public class DwsCommittable implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String jobId;
    private final long checkpointId;
    private final int subtaskId;
    private final String targetSchema;
    private final String targetTable;
    private final String stagingSchema;
    private final String stagingTable;
    private final List<String> columnNames;
    private final List<String> primaryKeys;

    public DwsCommittable(
            String jobId,
            long checkpointId,
            int subtaskId,
            String targetSchema,
            String targetTable,
            String stagingSchema,
            String stagingTable,
            List<String> columnNames,
            List<String> primaryKeys) {
        this.jobId = jobId;
        this.checkpointId = checkpointId;
        this.subtaskId = subtaskId;
        this.targetSchema = targetSchema;
        this.targetTable = targetTable;
        this.stagingSchema = stagingSchema;
        this.stagingTable = stagingTable;
        this.columnNames = new ArrayList<>(columnNames);
        this.primaryKeys = new ArrayList<>(primaryKeys);
    }

    public String getJobId() {
        return jobId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public String getTargetSchema() {
        return targetSchema;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getStagingSchema() {
        return stagingSchema;
    }

    public String getStagingTable() {
        return stagingTable;
    }

    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnNames);
    }

    public List<String> getPrimaryKeys() {
        return Collections.unmodifiableList(primaryKeys);
    }

    public String getTargetIdentifier() {
        return targetSchema + "." + targetTable;
    }
}
